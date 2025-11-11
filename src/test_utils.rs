use crate::bindings::IOrderBookV4::{EvaluableV3, IO, OrderV3};
use crate::bindings::IOrderBookV5::{EvaluableV4, IOV2, OrderV4};
use crate::offchain::execution::OffchainExecution;
use crate::onchain::OnchainTrade;
use crate::onchain::io::TokenizedEquitySymbol;
use alloy::primitives::{LogData, U256, address, bytes, fixed_bytes};
use alloy::rpc::types::Log;
use chrono::Utc;
use sqlx::SqlitePool;
use st0x_broker::OrderState;
use st0x_broker::schwab::{SchwabAuthEnv, SchwabTokens};
use st0x_broker::{Direction, Shares, SupportedBroker, Symbol};

/// Returns a test `OrderV3` instance (from IOrderBookV4) that is shared across
/// ClearV2 unit-tests. The exact values are not important – only that the
/// structure is valid and deterministic.
pub(crate) fn get_test_order() -> OrderV3 {
    OrderV3 {
        owner: address!("0xdddddddddddddddddddddddddddddddddddddddd"),
        evaluable: EvaluableV3 {
            interpreter: address!("0x2222222222222222222222222222222222222222"),
            store: address!("0x3333333333333333333333333333333333333333"),
            bytecode: bytes!("0x00"),
        },
        nonce: fixed_bytes!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
        validInputs: vec![
            IO {
                token: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                decimals: 6,
                vaultId: U256::from(0),
            },
            IO {
                token: address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                decimals: 18,
                vaultId: U256::from(0),
            },
        ],
        validOutputs: vec![
            IO {
                token: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                decimals: 6,
                vaultId: U256::from(0),
            },
            IO {
                token: address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                decimals: 18,
                vaultId: U256::from(0),
            },
        ],
    }
}

/// Returns a test `OrderV4` instance (from IOrderBookV5) for TakeOrderV3
/// unit-tests. The exact values are not important – only that the structure
/// is valid and deterministic.
pub(crate) fn get_test_order_v4() -> OrderV4 {
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
                vaultId: fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000000"),
            },
            IOV2 {
                token: address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                vaultId: fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000000"),
            },
        ],
        validOutputs: vec![
            IOV2 {
                token: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                vaultId: fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000000"),
            },
            IOV2 {
                token: address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                vaultId: fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000000"),
            },
        ],
    }
}

/// Creates a generic `Log` stub with the supplied log index. This helper is
/// useful when the concrete value of most fields is irrelevant for the
/// assertion being performed.
pub(crate) fn create_log(log_index: u64) -> Log {
    Log {
        inner: alloy::primitives::Log {
            address: address!("0xfefefefefefefefefefefefefefefefefefefefe"),
            data: LogData::empty(),
        },
        block_hash: None,
        block_number: Some(12345),
        block_timestamp: None,
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
pub(crate) fn get_test_log() -> Log {
    create_log(293)
}

/// Centralized test database setup to eliminate duplication across test files.
/// Creates an in-memory SQLite database with all migrations applied.
pub(crate) async fn setup_test_db() -> SqlitePool {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    sqlx::migrate!().run(&pool).await.unwrap();
    pool
}

/// Centralized test token setup to eliminate duplication across test files.
/// Creates and stores test tokens in the database for Schwab API authentication.
pub(crate) async fn setup_test_tokens(pool: &SqlitePool, env: &SchwabAuthEnv) {
    let tokens = SchwabTokens {
        access_token: "test_access_token".to_string(),
        access_token_fetched_at: Utc::now(),
        refresh_token: "test_refresh_token".to_string(),
        refresh_token_fetched_at: Utc::now(),
    };
    tokens.store(pool, &env.encryption_key).await.unwrap();
}

/// Builder for creating OnchainTrade test instances with sensible defaults.
/// Reduces duplication in test data setup.
pub(crate) struct OnchainTradeBuilder {
    trade: OnchainTrade,
}

impl Default for OnchainTradeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OnchainTradeBuilder {
    pub(crate) fn new() -> Self {
        Self {
            trade: OnchainTrade {
                id: None,
                tx_hash: fixed_bytes!(
                    "0x1111111111111111111111111111111111111111111111111111111111111111"
                ),
                log_index: 1,
                symbol: "AAPL0x".parse::<TokenizedEquitySymbol>().unwrap(),
                amount: 1.0,
                direction: Direction::Buy,
                price_usdc: 150.0,
                block_timestamp: None,
                created_at: None,
                gas_used: None,
                effective_gas_price: None,
                pyth_price: None,
                pyth_confidence: None,
                pyth_exponent: None,
                pyth_publish_time: None,
            },
        }
    }

    #[must_use]
    pub(crate) fn with_symbol(mut self, symbol: &str) -> Self {
        self.trade.symbol = symbol.parse::<TokenizedEquitySymbol>().unwrap();
        self
    }

    #[must_use]
    pub(crate) fn with_amount(mut self, amount: f64) -> Self {
        self.trade.amount = amount;
        self
    }

    #[must_use]
    pub(crate) fn with_price(mut self, price: f64) -> Self {
        self.trade.price_usdc = price;
        self
    }

    #[must_use]
    pub(crate) fn with_tx_hash(mut self, hash: alloy::primitives::B256) -> Self {
        self.trade.tx_hash = hash;
        self
    }

    #[must_use]
    pub(crate) fn with_log_index(mut self, index: u64) -> Self {
        self.trade.log_index = index;
        self
    }

    pub(crate) fn build(self) -> OnchainTrade {
        self.trade
    }
}

/// Builder for creating OffchainExecution test instances with sensible defaults.
/// Reduces duplication in test data setup.
pub(crate) struct OffchainExecutionBuilder {
    execution: OffchainExecution,
}

impl Default for OffchainExecutionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OffchainExecutionBuilder {
    pub(crate) fn new() -> Self {
        Self {
            execution: OffchainExecution {
                id: None,
                symbol: Symbol::new("AAPL").unwrap(),
                shares: Shares::new(100).unwrap(),
                direction: Direction::Buy,
                broker: SupportedBroker::Schwab,
                state: OrderState::Pending,
            },
        }
    }

    pub(crate) fn build(self) -> OffchainExecution {
        self.execution
    }
}
