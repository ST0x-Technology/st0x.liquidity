use alloy::network::EthereumWallet;
use alloy::node_bindings::AnvilInstance;
use alloy::primitives::{Address, B256, Bytes, U256, address, keccak256, utils::parse_units};
use alloy::providers::ProviderBuilder;
use alloy::providers::ext::AnvilApi as _;
use alloy::rpc::types::Log;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolEvent;
use chrono::Utc;
use cqrs_es::persist::GenericQuery;
use rain_math_float::Float;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
use sqlx::SqlitePool;
use st0x_execution::{
    Direction, FractionalShares, MockExecutor, OrderState, SupportedExecutor, Symbol,
};
use std::collections::HashMap;
use std::sync::Arc;

use super::{ExpectedEvent, assert_events, fetch_events};
use crate::alpaca_tokenization::tests::setup_anvil;
use crate::bindings::IOrderBookV5::{self, TakeOrderV3};
use crate::bindings::{
    DeployableERC20, Deployer, Interpreter, OrderBook, Parser, Store, TOFUTokenDecimals,
};
use crate::cctp::USDC_BASE;
use crate::conductor::{
    ExecutorOrderPlacer, TradeProcessingCqrs, VaultDiscoveryContext,
    check_and_execute_accumulated_positions, discover_vaults_for_trade, process_queued_trade,
};
use crate::error::EventProcessingError;
use crate::offchain::order_poller::{OrderPollerConfig, OrderStatusPoller};
use crate::offchain_order::{OffchainOrderAggregate, OffchainOrderCqrs, OffchainOrderId};
use crate::onchain::OnchainTrade;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::trade::TradeEvent;
use crate::position::{Position, PositionAggregate, PositionCqrs, PositionQuery, load_position};
use crate::queue::QueuedEvent;
use crate::symbol::cache::SymbolCache;
use crate::test_utils::setup_test_db;
use crate::threshold::ExecutionThreshold;
use crate::vault_registry::{VaultRegistry, VaultRegistryAggregate};

/// Loads a position and asserts it matches the expected field values.
///
/// The `last_updated` timestamp is non-deterministic and is asserted against the loaded value.
#[bon::builder]
async fn assert_position(
    query: &Arc<PositionQuery>,
    symbol: &Symbol,
    net: FractionalShares,
    accumulated_long: FractionalShares,
    accumulated_short: FractionalShares,
    pending: Option<OffchainOrderId>,
    last_price_usdc: Decimal,
) {
    let pos = load_position(query, symbol)
        .await
        .unwrap()
        .expect("Position should exist");
    assert_eq!(
        pos,
        Position {
            net,
            accumulated_long,
            accumulated_short,
            pending_offchain_order_id: pending,
            last_price_usdc: Some(last_price_usdc),
            last_updated: pos.last_updated,
        }
    );
}

/// A trade produced by a real OrderBook take-order on Anvil, containing the parsed
/// `OnchainTrade` and a matching `QueuedEvent` ready for CQRS processing.
struct AnvilTrade {
    trade: OnchainTrade,
    queued_event: QueuedEvent,
    tx_hash: B256,
    log_index: u64,
}

impl AnvilTrade {
    fn aggregate_id(&self) -> String {
        format!("{}:{}", self.tx_hash, self.log_index)
    }

    async fn submit(
        &self,
        pool: &SqlitePool,
        event_id: i64,
        cqrs: &TradeProcessingCqrs,
    ) -> Result<Option<OffchainOrderId>, EventProcessingError> {
        process_queued_trade(
            SupportedExecutor::DryRun,
            pool,
            &self.queued_event,
            event_id,
            self.trade.clone(),
            cqrs,
        )
        .await
    }
}

/// Creates a poller with the default MockExecutor (returns Filled) and polls pending orders.
async fn poll_and_fill(
    pool: &SqlitePool,
    offchain_order_cqrs: &Arc<OffchainOrderCqrs>,
    position_cqrs: &Arc<PositionCqrs>,
) -> Result<(), Box<dyn std::error::Error>> {
    let poller = OrderStatusPoller::new(
        OrderPollerConfig::default(),
        pool.clone(),
        MockExecutor::new(),
        offchain_order_cqrs.clone(),
        position_cqrs.clone(),
    );
    poller.poll_pending_orders().await?;
    Ok(())
}

/// Holds a deployed Rain OrderBook on a local Anvil node, ready to create real take-order events.
struct AnvilOrderBook<P> {
    _anvil: AnvilInstance,
    provider: P,
    orderbook_addr: Address,
    deployer_addr: Address,
    interpreter_addr: Address,
    store_addr: Address,
    owner: Address,
    usdc_addr: Address,
    equity_tokens: HashMap<String, Address>,
    symbol_cache: SymbolCache,
    feed_id_cache: FeedIdCache,
}

/// Places USDC contract code and storage directly at `USDC_BASE` via Anvil cheatcodes,
/// avoiding a temporary deploy + clone. Initializes the OpenZeppelin ERC20 storage layout:
/// totalSupply, name ("USD Coin"), symbol ("USDC"), decimals (6), and balance for `owner`.
async fn deploy_usdc_at_base<P: alloy::providers::Provider>(provider: &P, owner: Address) {
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

async fn setup_anvil_orderbook() -> AnvilOrderBook<impl alloy::providers::Provider + Clone> {
    let (anvil, endpoint, key) = setup_anvil();
    let signer = PrivateKeySigner::from_bytes(&key).unwrap();
    let wallet = EthereumWallet::from(signer.clone());
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect(&endpoint)
        .await
        .unwrap();
    let owner = signer.address();

    provider
        .anvil_set_code(
            address!("4f1C29FAAB7EDdF8D7794695d8259996734Cc665"),
            TOFUTokenDecimals::DEPLOYED_BYTECODE.clone(),
        )
        .await
        .unwrap();

    let interpreter = Interpreter::deploy(&provider).await.unwrap();
    let store = Store::deploy(&provider).await.unwrap();
    let parser = Parser::deploy(&provider).await.unwrap();
    let deployer = Deployer::deploy(
        &provider,
        Deployer::RainterpreterExpressionDeployerConstructionConfigV2 {
            interpreter: *interpreter.address(),
            store: *store.address(),
            parser: *parser.address(),
        },
    )
    .await
    .unwrap();

    let orderbook = OrderBook::deploy(&provider).await.unwrap();

    // Place USDC contract directly at USDC_BASE so vault discovery recognizes it.
    deploy_usdc_at_base(&provider, owner).await;

    // Extract addresses before moving provider into the struct
    let orderbook_addr = *orderbook.address();
    let deployer_addr = *deployer.address();
    let interpreter_addr = *interpreter.address();
    let store_addr = *store.address();

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
        feed_id_cache: FeedIdCache::default(),
    }
}

impl<P: alloy::providers::Provider + Clone> AnvilOrderBook<P> {
    /// Creates an order on the real OrderBook, takes it, and parses the resulting
    /// `TakeOrderV3` event through the full `OnchainTrade` pipeline.
    ///
    /// Equity tokens are deployed on first use per symbol and reused for subsequent calls.
    /// For Buy direction, only `price = 1` is supported (Rain expression ioRatio limitation).
    #[allow(clippy::too_many_lines)]
    async fn take_order(
        &mut self,
        symbol: &str,
        amount: f64,
        direction: Direction,
        price: u32,
    ) -> AnvilTrade {
        let orderbook =
            IOrderBookV5::IOrderBookV5Instance::new(self.orderbook_addr, &self.provider);
        let deployer_instance = Deployer::DeployerInstance::new(self.deployer_addr, &self.provider);

        // Deploy equity token on first use per symbol, reuse on subsequent calls
        if !self.equity_tokens.contains_key(symbol) {
            let equity = DeployableERC20::deploy(
                &self.provider,
                format!("{symbol} Tokenized"),
                format!("t{symbol}"),
                18,
                self.owner,
                parse_units("1000000", 18).unwrap().into(),
            )
            .await
            .unwrap();

            self.equity_tokens
                .insert(symbol.to_string(), *equity.address());
        }

        let is_sell = direction == Direction::Sell;
        let usdc_addr = self.usdc_addr;
        let equity_addr = self.equity_tokens[symbol];
        let usdc_total = amount * f64::from(price);

        // Format amounts with fixed precision to avoid f64 representation artifacts
        let amount_str = format!("{amount:.6}");
        let usdc_total_str = format!("{usdc_total:.6}");

        // Order: input = what order receives, output = what order gives
        let (input_token, output_token) = if is_sell {
            (usdc_addr, equity_addr)
        } else {
            (equity_addr, usdc_addr)
        };

        // Expression: maxAmount (output in base units) and ioRatio (as Float)
        // Sell: maxAmount = shares in 18-dec, ioRatio = price
        // Buy: maxAmount = USDC in 6-dec, ioRatio = 1/price (only price=1 supported)
        let (max_amount_base, io_ratio_str) = if is_sell {
            let base: U256 = parse_units(&amount_str, 18).unwrap().into();
            (base, price.to_string())
        } else {
            let base: U256 = parse_units(&usdc_total_str, 6).unwrap().into();
            (base, "1".to_string())
        };
        let expression = format!("_ _: {max_amount_base} {io_ratio_str};:;");

        let parsed_bytecode = deployer_instance
            .parse2(Bytes::copy_from_slice(expression.as_bytes()))
            .call()
            .await
            .unwrap()
            .0;

        let input_vault_id = B256::from(keccak256(symbol.as_bytes()));
        let output_vault_id = B256::from(keccak256(symbol.as_bytes()));

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

        let add_order_receipt = orderbook
            .addOrder3(order_config, vec![])
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
            .find_map(|log| log.log_decode::<IOrderBookV5::AddOrderV3>().ok())
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
            .deposit3(output_token, output_vault_id, deposit_float, vec![])
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

        // Take the order
        let take_config = IOrderBookV5::TakeOrdersConfigV4 {
            minimumInput: B256::ZERO,
            maximumInput: Float::from_fixed_decimal_lossy(U256::from(1_000_000), 0)
                .unwrap()
                .get_inner(),
            maximumIORatio: Float::from_fixed_decimal_lossy(U256::from(1_000_000), 0)
                .unwrap()
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
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        assert!(
            take_receipt.status(),
            "takeOrders3 reverted. Logs: {:?}",
            take_receipt.inner.logs()
        );

        // Extract and parse TakeOrderV3 event
        let take_log = take_receipt
            .inner
            .logs()
            .iter()
            .find(|log| log.topic0() == Some(&TakeOrderV3::SIGNATURE_HASH))
            .expect("TakeOrderV3 event not found");

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

        let trade = OnchainTrade::try_from_take_order_if_target_owner(
            &self.symbol_cache,
            &self.provider,
            take_event,
            log_metadata,
            order.owner,
            &self.feed_id_cache,
        )
        .await
        .unwrap()
        .expect("Pipeline should produce an OnchainTrade");

        let tx_hash = trade.tx_hash;
        let log_index = trade.log_index;

        let queued_event = QueuedEvent {
            id: Some(1),
            tx_hash,
            log_index,
            block_number: take_log.block_number.unwrap_or(1),
            event: TradeEvent::TakeOrderV3(Box::new(take_event_for_queue)),
            processed: false,
            created_at: None,
            processed_at: None,
            block_timestamp: trade.block_timestamp,
        };

        AnvilTrade {
            trade,
            queued_event,
            tx_hash,
            log_index,
        }
    }
}

/// Constructs the CQRS frameworks needed by the integration tests.
///
/// Uses `ExecutorOrderPlacer(MockExecutor::new())` so that the `PlaceOrder`
/// command atomically calls the mock executor and emits `Placed` + `Submitted`.
fn create_test_cqrs(
    pool: &SqlitePool,
) -> (
    TradeProcessingCqrs,
    Arc<PositionCqrs>,
    Arc<PositionQuery>,
    Arc<OffchainOrderCqrs>,
) {
    let onchain_trade_cqrs = Arc::new(sqlite_cqrs(pool.clone(), vec![], ()));

    let position_view_repo = Arc::new(
        SqliteViewRepository::<PositionAggregate, PositionAggregate>::new(
            pool.clone(),
            "position_view".to_string(),
        ),
    );
    let position_query = Arc::new(GenericQuery::new(position_view_repo.clone()));
    let position_cqrs: Arc<PositionCqrs> = Arc::new(sqlite_cqrs(
        pool.clone(),
        vec![Box::new(GenericQuery::new(position_view_repo))],
        (),
    ));

    let order_placer: crate::offchain_order::OffchainOrderServices =
        Arc::new(ExecutorOrderPlacer(MockExecutor::new()));

    let offchain_order_view_repo = Arc::new(SqliteViewRepository::<
        OffchainOrderAggregate,
        OffchainOrderAggregate,
    >::new(
        pool.clone(), "offchain_order_view".to_string()
    ));
    let offchain_order_cqrs: Arc<OffchainOrderCqrs> = Arc::new(sqlite_cqrs(
        pool.clone(),
        vec![Box::new(GenericQuery::new(offchain_order_view_repo))],
        order_placer,
    ));

    let cqrs = TradeProcessingCqrs {
        onchain_trade_cqrs,
        position_cqrs: position_cqrs.clone(),
        position_query: position_query.clone(),
        offchain_order_cqrs: offchain_order_cqrs.clone(),
        execution_threshold: ExecutionThreshold::whole_share(),
    };

    (cqrs, position_cqrs, position_query, offchain_order_cqrs)
}

#[tokio::test]
async fn onchain_trades_accumulate_and_trigger_offchain_fill()
-> Result<(), Box<dyn std::error::Error>> {
    let mut ob = setup_anvil_orderbook().await;
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);
    let symbol = Symbol::new("AAPL").unwrap();

    // Checkpoint 1: before any trades -- no Position aggregate exists
    assert!(load_position(&position_query, &symbol).await?.is_none());

    // Trade 1: 0.5 shares Sell, below whole-share threshold
    let t1 = ob.take_order("AAPL", 0.5, Direction::Sell, 100).await;
    let t1_agg = t1.aggregate_id();
    let result1 = t1.submit(&pool, 1, &cqrs).await?;
    assert!(
        result1.is_none(),
        "No execution should be created below threshold"
    );

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(dec!(-0.5)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(0.5)))
        .last_price_usdc(dec!(100))
        .call()
        .await;

    let mut expected = vec![
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &t1_agg, "OnChainTradeEvent::Filled"),
    ];
    assert_events(&pool, &expected).await;

    // Trade 2: 0.7 shares Sell, total net = -1.2, crosses threshold
    let t2 = ob.take_order("AAPL", 0.7, Direction::Sell, 100).await;
    let t2_agg = t2.aggregate_id();
    let order_id = t2
        .submit(&pool, 2, &cqrs)
        .await?
        .expect("Threshold crossed, should return OffchainOrderId");
    let order_id_str = order_id.to_string();

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(dec!(-1.2)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(1.2)))
        .pending(order_id)
        .last_price_usdc(dec!(100))
        .call()
        .await;

    expected.extend([
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &t2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order_id_str,
            "OffchainOrderEvent::Submitted",
        ),
    ]);
    let events = assert_events(&pool, &expected).await;

    // Payload spot-checks for post-threshold events
    let placed_pos = &events[4].payload["OffChainOrderPlaced"];
    assert_eq!(
        placed_pos["offchain_order_id"].as_str().unwrap(),
        order_id_str
    );
    assert_eq!(placed_pos["direction"].as_str().unwrap(), "Buy");

    let offchain_placed = &events[5].payload["Placed"];
    assert_eq!(offchain_placed["symbol"].as_str().unwrap(), "AAPL");
    assert_eq!(offchain_placed["direction"].as_str().unwrap(), "Buy");

    // Fulfillment: order poller detects the filled order and completes the lifecycle
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(1.2)))
        .last_price_usdc(dec!(100))
        .call()
        .await;

    expected.extend([
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFilled"),
    ]);
    let events = assert_events(&pool, &expected).await;
    assert_eq!(
        events[8].payload["OffChainOrderFilled"]["offchain_order_id"]
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
    let mut ob = setup_anvil_orderbook().await;
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);
    let symbol = Symbol::new("AAPL").unwrap();

    // Two trades: 0.5 + 0.7 = 1.2 shares, crosses threshold
    let t1 = ob.take_order("AAPL", 0.5, Direction::Sell, 100).await;
    let t1_agg = t1.aggregate_id();
    t1.submit(&pool, 1, &cqrs).await?;

    let t2 = ob.take_order("AAPL", 0.7, Direction::Sell, 100).await;
    let t2_agg = t2.aggregate_id();
    let order_id = t2
        .submit(&pool, 2, &cqrs)
        .await?
        .expect("Threshold crossed");
    let order_id_str = order_id.to_string();

    // Poller discovers the broker FAILED the order
    let failed_executor = MockExecutor::new().with_order_status(OrderState::Failed {
        failed_at: Utc::now(),
        error_reason: Some("Broker rejected order".to_string()),
    });
    OrderStatusPoller::new(
        OrderPollerConfig::default(),
        pool.clone(),
        failed_executor,
        offchain_order_cqrs.clone(),
        position_cqrs.clone(),
    )
    .poll_pending_orders()
    .await?;

    // After failure: pending cleared, position still has net exposure
    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(dec!(-1.2)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(1.2)))
        .last_price_usdc(dec!(100))
        .call()
        .await;

    let mut expected = vec![
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &t1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &t2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order_id_str,
            "OffchainOrderEvent::Submitted",
        ),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Failed"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFailed"),
    ];
    let events = assert_events(&pool, &expected).await;
    let placed = &events[5].payload["Placed"];
    assert_eq!(placed["symbol"].as_str().unwrap(), "AAPL");
    assert_eq!(placed["direction"].as_str().unwrap(), "Buy");

    // Position checker finds the unexecuted position and retries
    check_and_execute_accumulated_positions(
        &MockExecutor::new(),
        &pool,
        &position_cqrs,
        &position_query,
        &offchain_order_cqrs,
        &ExecutionThreshold::whole_share(),
    )
    .await?;

    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    // Final: position fully hedged
    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(1.2)))
        .last_price_usdc(dec!(100))
        .call()
        .await;

    // Extract retry order ID from the new OffchainOrderEvent::Placed event
    let all_events = fetch_events(&pool).await;
    let retry_id = all_events[10].aggregate_id.clone();
    assert_ne!(
        retry_id, order_id_str,
        "Retry should create a new offchain order"
    );

    expected.extend([
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &retry_id, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new("OffchainOrder", &retry_id, "OffchainOrderEvent::Submitted"),
        ExpectedEvent::new("OffchainOrder", &retry_id, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFilled"),
    ]);
    assert_events(&pool, &expected).await;

    Ok(())
}

/// Tests that two symbols processed through the pipeline don't contaminate each other's
/// Position state or event streams.
#[tokio::test]
async fn multi_symbol_isolation() -> Result<(), Box<dyn std::error::Error>> {
    let mut ob = setup_anvil_orderbook().await;
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);
    let aapl = Symbol::new("AAPL").unwrap();
    let msft = Symbol::new("MSFT").unwrap();

    // Phase 1: interleaved trades -- AAPL sell 0.6, MSFT sell 0.4, AAPL sell 0.6
    let t1 = ob.take_order("AAPL", 0.6, Direction::Sell, 100).await;
    let t1_agg = t1.aggregate_id();
    assert!(t1.submit(&pool, 1, &cqrs).await?.is_none());

    let t2 = ob.take_order("MSFT", 0.4, Direction::Sell, 200).await;
    let t2_agg = t2.aggregate_id();
    assert!(t2.submit(&pool, 2, &cqrs).await?.is_none());

    let t3 = ob.take_order("AAPL", 0.6, Direction::Sell, 100).await;
    let t3_agg = t3.aggregate_id();
    let aapl_order_id = t3
        .submit(&pool, 3, &cqrs)
        .await?
        .expect("AAPL 1.2 crosses threshold");
    let aapl_order_str = aapl_order_id.to_string();

    assert_position()
        .query(&position_query)
        .symbol(&aapl)
        .net(FractionalShares::new(dec!(-1.2)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(1.2)))
        .pending(aapl_order_id)
        .last_price_usdc(dec!(100))
        .call()
        .await;
    assert_position()
        .query(&position_query)
        .symbol(&msft)
        .net(FractionalShares::new(dec!(-0.4)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(0.4)))
        .last_price_usdc(dec!(200))
        .call()
        .await;

    let mut expected = vec![
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &t1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "MSFT", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &t2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &t3_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new(
            "OffchainOrder",
            &aapl_order_str,
            "OffchainOrderEvent::Placed",
        ),
        ExpectedEvent::new(
            "OffchainOrder",
            &aapl_order_str,
            "OffchainOrderEvent::Submitted",
        ),
    ];
    let events = assert_events(&pool, &expected).await;

    let aapl_placed = &events[7].payload["Placed"];
    assert_eq!(aapl_placed["symbol"].as_str().unwrap(), "AAPL");
    assert_eq!(aapl_placed["direction"].as_str().unwrap(), "Buy");

    // Poll and fill AAPL, verify MSFT unchanged
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    assert_position()
        .query(&position_query)
        .symbol(&aapl)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(1.2)))
        .last_price_usdc(dec!(100))
        .call()
        .await;
    assert_position()
        .query(&position_query)
        .symbol(&msft)
        .net(FractionalShares::new(dec!(-0.4)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(0.4)))
        .last_price_usdc(dec!(200))
        .call()
        .await;

    expected.extend([
        ExpectedEvent::new(
            "OffchainOrder",
            &aapl_order_str,
            "OffchainOrderEvent::Filled",
        ),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFilled"),
    ]);
    assert_events(&pool, &expected).await;

    // MSFT crosses threshold (0.4 + 0.6 = 1.0, exactly at threshold)
    let t4 = ob.take_order("MSFT", 0.6, Direction::Sell, 200).await;
    let t4_agg = t4.aggregate_id();
    let msft_order_id = t4
        .submit(&pool, 4, &cqrs)
        .await?
        .expect("MSFT 1.0 hits threshold");
    let msft_order_str = msft_order_id.to_string();

    assert_position()
        .query(&position_query)
        .symbol(&msft)
        .net(FractionalShares::new(dec!(-1.0)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(1.0)))
        .pending(msft_order_id)
        .last_price_usdc(dec!(200))
        .call()
        .await;
    assert_ne!(
        aapl_order_id, msft_order_id,
        "Separate offchain orders per symbol"
    );

    expected.extend([
        ExpectedEvent::new("Position", "MSFT", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &t4_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "MSFT", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new(
            "OffchainOrder",
            &msft_order_str,
            "OffchainOrderEvent::Placed",
        ),
        ExpectedEvent::new(
            "OffchainOrder",
            &msft_order_str,
            "OffchainOrderEvent::Submitted",
        ),
    ]);
    let events = assert_events(&pool, &expected).await;

    let msft_placed = &events[14].payload["Placed"];
    assert_eq!(msft_placed["symbol"].as_str().unwrap(), "MSFT");
    assert_eq!(msft_placed["direction"].as_str().unwrap(), "Buy");

    // Poll and fill MSFT, both positions end fully hedged
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    assert_position()
        .query(&position_query)
        .symbol(&aapl)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(1.2)))
        .last_price_usdc(dec!(100))
        .call()
        .await;
    assert_position()
        .query(&position_query)
        .symbol(&msft)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(1.0)))
        .last_price_usdc(dec!(200))
        .call()
        .await;

    expected.extend([
        ExpectedEvent::new(
            "OffchainOrder",
            &msft_order_str,
            "OffchainOrderEvent::Filled",
        ),
        ExpectedEvent::new("Position", "MSFT", "PositionEvent::OffChainOrderFilled"),
    ]);
    assert_events(&pool, &expected).await;

    Ok(())
}

/// Tests that Buy direction onchain trades accumulate `accumulated_long` and produce a
/// Sell hedge when the position crosses threshold.
#[tokio::test]
async fn buy_direction_accumulates_long() -> Result<(), Box<dyn std::error::Error>> {
    let mut ob = setup_anvil_orderbook().await;
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);
    let symbol = Symbol::new("AAPL").unwrap();

    // Trade 1: Buy 0.5 shares, below threshold
    let t1 = ob.take_order("AAPL", 0.5, Direction::Buy, 1).await;
    let t1_agg = t1.aggregate_id();
    let result1 = t1.submit(&pool, 1, &cqrs).await?;
    assert!(result1.is_none(), "Below threshold");

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(dec!(0.5)))
        .accumulated_long(FractionalShares::new(dec!(0.5)))
        .accumulated_short(FractionalShares::ZERO)
        .last_price_usdc(dec!(1))
        .call()
        .await;

    // Trade 2: Buy 0.7 shares, crosses threshold -> hedge is Sell
    let t2 = ob.take_order("AAPL", 0.7, Direction::Buy, 1).await;
    let t2_agg = t2.aggregate_id();
    let order_id = t2
        .submit(&pool, 2, &cqrs)
        .await?
        .expect("Threshold crossed");
    let order_id_str = order_id.to_string();

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(dec!(1.2)))
        .accumulated_long(FractionalShares::new(dec!(1.2)))
        .accumulated_short(FractionalShares::ZERO)
        .pending(order_id)
        .last_price_usdc(dec!(1))
        .call()
        .await;

    // Verify offchain order is Sell direction (hedge for long position)
    let mut expected = vec![
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &t1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &t2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order_id_str,
            "OffchainOrderEvent::Submitted",
        ),
    ];
    let events = assert_events(&pool, &expected).await;

    // Hedge direction should be Sell (opposite of onchain Buy)
    assert_eq!(
        events[5].payload["Placed"]["direction"].as_str().unwrap(),
        "Sell"
    );

    // Fill the hedge order
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::new(dec!(1.2)))
        .accumulated_short(FractionalShares::ZERO)
        .last_price_usdc(dec!(1))
        .call()
        .await;

    expected.extend([
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFilled"),
    ]);
    assert_events(&pool, &expected).await;

    Ok(())
}

/// Tests that a single trade of exactly 1.0 shares immediately triggers execution.
#[tokio::test]
async fn exact_threshold_triggers_execution() -> Result<(), Box<dyn std::error::Error>> {
    let mut ob = setup_anvil_orderbook().await;
    let pool = setup_test_db().await;
    let (cqrs, _position_cqrs, position_query, _offchain_order_cqrs) = create_test_cqrs(&pool);
    let symbol = Symbol::new("AAPL").unwrap();

    let t1 = ob.take_order("AAPL", 1.0, Direction::Sell, 100).await;
    let t1_agg = t1.aggregate_id();
    let order_id = t1
        .submit(&pool, 1, &cqrs)
        .await?
        .expect("Exactly 1.0 should cross whole-share threshold");

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(dec!(-1.0)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(1.0)))
        .pending(order_id)
        .last_price_usdc(dec!(100))
        .call()
        .await;

    let order_id_str = order_id.to_string();
    let expected = vec![
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &t1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order_id_str,
            "OffchainOrderEvent::Submitted",
        ),
    ];
    assert_events(&pool, &expected).await;

    Ok(())
}

/// Tests that the position checker is a no-op when all positions are already hedged.
#[tokio::test]
async fn position_checker_noop_when_hedged() -> Result<(), Box<dyn std::error::Error>> {
    let mut ob = setup_anvil_orderbook().await;
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);

    // Complete a full hedge cycle so position net=0
    let t1 = ob.take_order("AAPL", 1.0, Direction::Sell, 100).await;
    t1.submit(&pool, 1, &cqrs)
        .await?
        .expect("Threshold crossed");
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    let events_before = fetch_events(&pool).await;

    // Position checker should find nothing to do
    check_and_execute_accumulated_positions(
        &MockExecutor::new(),
        &pool,
        &position_cqrs,
        &position_query,
        &offchain_order_cqrs,
        &ExecutionThreshold::whole_share(),
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
    let mut ob = setup_anvil_orderbook().await;
    let pool = setup_test_db().await;
    let (cqrs, position_cqrs, position_query, offchain_order_cqrs) = create_test_cqrs(&pool);
    let symbol = Symbol::new("AAPL").unwrap();

    // First cycle: 1.0 share sell -> hedge -> fill
    let t1 = ob.take_order("AAPL", 1.0, Direction::Sell, 100).await;
    let t1_agg = t1.aggregate_id();
    let order1 = t1
        .submit(&pool, 1, &cqrs)
        .await?
        .expect("First threshold crossing");
    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(1.0)))
        .last_price_usdc(dec!(100))
        .call()
        .await;

    // Second cycle: another 1.5 share sell -> crosses threshold again
    let t2 = ob.take_order("AAPL", 1.5, Direction::Sell, 100).await;
    let t2_agg = t2.aggregate_id();
    let order2 = t2
        .submit(&pool, 2, &cqrs)
        .await?
        .expect("Second threshold crossing");
    assert_ne!(order1, order2, "Second cycle should create a new order");

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(dec!(-1.5)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(2.5)))
        .pending(order2)
        .last_price_usdc(dec!(100))
        .call()
        .await;

    poll_and_fill(&pool, &offchain_order_cqrs, &position_cqrs).await?;

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(dec!(2.5)))
        .last_price_usdc(dec!(100))
        .call()
        .await;

    // Verify the full event sequence across both cycles
    let order1_str = order1.to_string();
    let order2_str = order2.to_string();
    let expected = vec![
        // First cycle
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &t1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order1_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order1_str,
            "OffchainOrderEvent::Submitted",
        ),
        ExpectedEvent::new("OffchainOrder", &order1_str, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFilled"),
        // Second cycle
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("OnChainTrade", &t2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order2_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order2_str,
            "OffchainOrderEvent::Submitted",
        ),
        ExpectedEvent::new("OffchainOrder", &order2_str, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", "AAPL", "PositionEvent::OffChainOrderFilled"),
    ];
    assert_events(&pool, &expected).await;

    Ok(())
}

#[tokio::test]
async fn take_order_discovers_equity_vault() -> Result<(), Box<dyn std::error::Error>> {
    let mut ob = setup_anvil_orderbook().await;
    let pool = setup_test_db().await;
    let (cqrs, _, _, _) = create_test_cqrs(&pool);

    let t1 = ob.take_order("AAPL", 1.0, Direction::Sell, 100).await;
    t1.submit(&pool, 1, &cqrs).await?;

    // Run vault discovery using the same trade data
    let vault_registry_cqrs = sqlite_cqrs::<VaultRegistryAggregate>(pool.clone(), vec![], ());
    let context = VaultDiscoveryContext {
        vault_registry_cqrs: &vault_registry_cqrs,
        orderbook: ob.orderbook_addr,
        order_owner: ob.owner,
    };

    discover_vaults_for_trade(&t1.queued_event, &t1.trade, &context).await?;

    let vault_agg_id = VaultRegistry::aggregate_id(ob.orderbook_addr, ob.owner);
    let events = fetch_events(&pool).await;

    // The trade produces Position + OnChainTrade + offchain order events,
    // followed by VaultRegistry discovery for both the USDC and equity vaults.
    let vault_events: Vec<_> = events
        .iter()
        .filter(|e| e.aggregate_type == "VaultRegistry")
        .collect();

    assert_eq!(vault_events.len(), 2, "Expected USDC + equity vault events");

    // Both events belong to the same VaultRegistry aggregate
    for ve in &vault_events {
        assert_eq!(ve.aggregate_id, vault_agg_id);
    }

    // Sell order: input=USDC, output=equity. Vault discovery processes input first.
    assert_eq!(
        vault_events[0].event_type,
        "VaultRegistryEvent::UsdcVaultDiscovered"
    );
    assert_eq!(
        vault_events[0].payload["UsdcVaultDiscovered"]["vault_id"]
            .as_str()
            .unwrap(),
        format!("{:#x}", keccak256(b"AAPL"))
    );

    assert_eq!(
        vault_events[1].event_type,
        "VaultRegistryEvent::EquityVaultDiscovered"
    );

    let equity_payload = &vault_events[1].payload["EquityVaultDiscovered"];
    assert_eq!(equity_payload["symbol"].as_str().unwrap(), "AAPL");
    assert_eq!(
        equity_payload["vault_id"].as_str().unwrap(),
        format!("{:#x}", keccak256(b"AAPL"))
    );
    assert_eq!(
        equity_payload["token"].as_str().unwrap(),
        format!("{:#x}", ob.equity_tokens["AAPL"])
    );

    Ok(())
}
