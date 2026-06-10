//! Shared test fixtures: database setup, stub orders/logs,
//! and builders for onchain trades and offchain executions.

use alloy::network::TransactionBuilder;
use alloy::primitives::{Address, B256, LogData, address, bytes, fixed_bytes};
use alloy::providers::Provider;
use alloy::providers::ext::AnvilApi as _;
use alloy::rpc::types::{Log, TransactionRequest};
use chrono::Utc;
use rain_math_float::Float;
use sqlx::SqlitePool;

use st0x_execution::{Direction, FractionalShares, Positive};

use crate::bindings::IRaindexV6::{EvaluableV4, IOV2, OrderV4};
use crate::onchain::OnchainTrade;
use crate::onchain::io::{TokenizedSymbol, Usdc, WrappedTokenizedShares};

/// Deterministic singleton address of the TOFUTokenDecimals contract. The
/// orderbook's `LibTOFUTokenDecimals.ensureDeployed` hardcodes this address and
/// checks the codehash, so any test exercising deposits, withdrawals, or order
/// takes must place the canonical runtime here.
pub(crate) const TOFU_TOKEN_DECIMALS: Address =
    address!("0x200e12D10bb0c5E4a17e7018f0F1161919bb9389");

/// Canonical TOFUTokenDecimals init bytecode, copied from
/// rain-tofu-erc20-decimals' `LibTOFUTokenDecimals.TOFU_DECIMALS_EXPECTED_CREATION_CODE`.
/// Deploying this and etching the resulting runtime at `TOFU_TOKEN_DECIMALS` yields the
/// codehash `ensureDeployed` requires; rain.orderbook's own recompile of TOFUTokenDecimals.sol
/// does not match that hash, so its artifact bytecode cannot be used directly.
const TOFU_DECIMALS_CREATION_CODE: &str = "0x6080604052348015600e575f80fd5b5061044b8061001c5f395ff3fe608060405234801561000f575f80fd5b506004361061004a575f3560e01c80630782d7e11461004e57806354636d2b14610078578063b7bad1b11461009d578063f5c36eaf146100b0575b5f80fd5b61006161005c366004610363565b6100c3565b60405161006f929190610403565b60405180910390f35b61008b610086366004610363565b6100d8565b60405160ff909116815260200161006f565b6100616100ab366004610363565b6100e9565b61008b6100be366004610363565b6100f5565b5f806100cf5f84610100565b91509150915091565b5f6100e35f836101f0565b92915050565b5f806100cf5f84610281565b5f6100e35f83610356565b73ffffffffffffffffffffffffffffffffffffffff81165f9081526020838152604080832081518083019092525460ff8082161515835261010090910416818301527f313ce56700000000000000000000000000000000000000000000000000000000808452839283908190816004818a5afa915060203d1015610182575f91505b811561019857505f5160ff811115610198575f91505b816101af57505050602001516003925090506101e9565b83516101c3575f955093506101e992505050565b836020015160ff1681146101d85760026101db565b60015b846020015195509550505050505b9250929050565b5f805f6101fd8585610281565b909250905060018260038111156102165761021661039d565b1415801561023557505f8260038111156102325761023261039d565b14155b156102795783826040517fee07877f000000000000000000000000000000000000000000000000000000008152600401610270929190610421565b60405180910390fd5b949350505050565b5f805f8061028f8686610100565b90925090505f8260038111156102a7576102a761039d565b0361034b576040805180820182526001815260ff838116602080840191825273ffffffffffffffffffffffffffffffffffffffff8a165f908152908b9052939093209151825493517fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00009094169015157fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00ff161761010093909116929092029190911790555b909590945092505050565b5f805f6101fd8585610100565b5f60208284031215610373575f80fd5b813573ffffffffffffffffffffffffffffffffffffffff81168114610396575f80fd5b9392505050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52602160045260245ffd5b600481106103ff577f4e487b71000000000000000000000000000000000000000000000000000000005f52602160045260245ffd5b9052565b6040810161041182856103ca565b60ff831660208301529392505050565b73ffffffffffffffffffffffffffffffffffffffff831681526040810161039660208301846103ca56";

/// Deploys the canonical TOFUTokenDecimals init bytecode and etches the resulting
/// runtime at [`TOFU_TOKEN_DECIMALS`]. The orderbook checks both the address and
/// the codehash, so the runtime must come from executing the canonical creation
/// code rather than from a recompiled artifact.
pub(crate) async fn deploy_tofu_singleton<P: Provider>(provider: &P) {
    let creation_code = alloy::hex::decode(TOFU_DECIMALS_CREATION_CODE).unwrap();
    let deployed = provider
        .send_transaction(TransactionRequest::default().with_deploy_code(creation_code))
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
    crate::conductor::setup_apalis_tables(&pool).await.unwrap();
    pool
}

/// Shared constructor for positive share quantities in tests.
pub(crate) fn positive_shares(value: &str) -> Positive<FractionalShares> {
    Positive::new(FractionalShares::new(
        Float::parse(value.to_string()).unwrap(),
    ))
    .unwrap()
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
                tx_hash: fixed_bytes!(
                    "0x1111111111111111111111111111111111111111111111111111111111111111"
                ),
                log_index: 1,
                symbol: "wtAAPL"
                    .parse::<TokenizedSymbol<WrappedTokenizedShares>>()
                    .unwrap(),
                equity_token: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                amount: FractionalShares::new(Float::parse("1".to_string()).unwrap()),
                direction: Direction::Buy,
                price: Usdc::new(Float::parse("150".to_string()).unwrap()).unwrap(),
                block_timestamp: Some(Utc::now()),
                gas_used: None,
                effective_gas_price: None,
                pyth_price: None,
            },
        }
    }

    #[must_use]
    pub(crate) fn with_symbol(mut self, symbol: &str) -> Self {
        self.trade.symbol = symbol
            .parse::<TokenizedSymbol<WrappedTokenizedShares>>()
            .unwrap();
        self
    }

    #[must_use]
    pub(crate) fn with_equity_token(mut self, token: Address) -> Self {
        self.trade.equity_token = token;
        self
    }

    #[must_use]
    pub(crate) fn with_amount(mut self, amount: Float) -> Self {
        self.trade.amount = FractionalShares::new(amount);
        self
    }

    #[must_use]
    pub(crate) fn with_log_index(mut self, index: u64) -> Self {
        self.trade.log_index = index;
        self
    }

    #[must_use]
    pub(crate) fn with_enrichment(
        mut self,
        gas_used: u64,
        effective_gas_price: u128,
        pyth_price: crate::onchain_trade::PythPrice,
    ) -> Self {
        self.trade.gas_used = Some(gas_used);
        self.trade.effective_gas_price = Some(effective_gas_price);
        self.trade.pyth_price = Some(pyth_price);
        self
    }

    pub(crate) fn build(self) -> OnchainTrade {
        self.trade
    }
}
