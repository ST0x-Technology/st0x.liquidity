//! Shared test fixtures: database setup, stub orders/logs,
//! and builders for onchain trades and offchain executions.

use alloy::primitives::{Address, B256, LogData, address, bytes, fixed_bytes};
use alloy::rpc::types::Log;
use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::SqlitePool;

use st0x_execution::{Direction, FractionalShares, SchwabTokens};

use crate::bindings::IOrderBookV5::{EvaluableV4, IOV2, OrderV4};
use crate::config::SchwabAuth;
use crate::onchain::OnchainTrade;
use crate::onchain::io::{TokenizedEquitySymbol, Usdc};

/// Returns a test `OrderV4` instance that is shared across multiple
/// unit-tests. The exact values are not important -- only that the
/// structure is valid and deterministic.
pub(crate) fn get_test_order() -> OrderV4 {
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

/// Centralized test token setup to eliminate duplication
/// across test files. Creates and stores test tokens in
/// the database for Schwab API authentication.
pub(crate) async fn setup_test_tokens(pool: &SqlitePool, auth: &SchwabAuth) {
    let tokens = SchwabTokens {
        access_token: "test_access_token".to_string(),
        access_token_fetched_at: Utc::now(),
        refresh_token: "test_refresh_token".to_string(),
        refresh_token_fetched_at: Utc::now(),
    };
    tokens.store(pool, &auth.encryption_key).await.unwrap();
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
                symbol: "tAAPL".parse::<TokenizedEquitySymbol>().unwrap(),
                equity_token: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                amount: FractionalShares::new(Decimal::ONE),
                direction: Direction::Buy,
                price: Usdc::new(Decimal::new(150, 0)).unwrap(),
                block_timestamp: Some(Utc::now()),
                created_at: None,
                gas_used: None,
                effective_gas_price: None,
                pyth_price: None,
                pyth_confidence: None,
                pyth_exponent: None,
                pyth_publish_time: None,
                vault_ratio: None,
            },
        }
    }

    #[must_use]
    pub(crate) fn with_symbol(mut self, symbol: &str) -> Self {
        self.trade.symbol = symbol.parse::<TokenizedEquitySymbol>().unwrap();
        self
    }

    #[must_use]
    pub(crate) fn with_equity_token(mut self, token: Address) -> Self {
        self.trade.equity_token = token;
        self
    }

    #[must_use]
    pub(crate) fn with_amount(mut self, amount: Decimal) -> Self {
        self.trade.amount = FractionalShares::new(amount);
        self
    }

    #[must_use]
    pub(crate) fn with_price(mut self, price: Decimal) -> Self {
        self.trade.price = Usdc::new(price).unwrap();
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
