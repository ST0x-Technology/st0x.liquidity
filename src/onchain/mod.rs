//! Onchain event processing: trade parsing, event backfilling,
//! position accumulation, and vault management.

use alloy::primitives::ruint::FromUintError;
use alloy::transports::{RpcError, TransportErrorKind};
use rain_math_float::FloatError;
use std::num::TryFromIntError;

use st0x_event_sorcery::ProjectionError;
use st0x_execution::order::status::ParseOrderStatusError;
use st0x_execution::{
    EmptySymbolError, ExecutionError, FractionalShares, InvalidDirectionError,
    InvalidExecutorError, InvalidSharesError, NotPositive, PersistenceError, SharesConversionError,
};

use crate::position::{Position, PositionError};

/// The single pre-multichain compatibility contract: every payload persisted
/// before chains were tracked -- fill events, in-flight jobs, inventory
/// snapshots, hydrated views -- happened on Base, the only chain that
/// existed. Serde defaults for legacy data all resolve through here.
pub(crate) fn legacy_chain() -> st0x_evm::Chain {
    st0x_evm::Chain::Base
}

pub(crate) mod accumulator;
pub(crate) mod approvals;
pub(crate) mod backfill;
mod clear;
pub(crate) mod io;
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod mock;
mod take_order;
pub(crate) mod trade;

pub use trade::{OnchainTrade, TradeValidationError};

/// Builds the [`st0x_raindex::RaindexContracts`] pair from the EVM config.
/// The single conversion point keeps every construction site in sync should
/// the struct grow a field.
pub fn raindex_contracts(
    trading_chain: &st0x_config::TradingChain,
) -> st0x_raindex::RaindexContracts {
    st0x_raindex::RaindexContracts {
        inventory: trading_chain.inventory_address(),
        orderbook: trading_chain.orderbook,
    }
}

/// Unified error type for onchain trade processing with clear domain boundaries.
/// Provides error mapping between layers while maintaining separation of concerns.
#[derive(Debug, thiserror::Error)]
pub enum OnChainError {
    #[error("job addressed to chain {chain}, which no watched chain matches")]
    UnwatchedChain { chain: st0x_evm::Chain },
    #[error("Trade validation error: {0}")]
    Validation(#[from] TradeValidationError),
    #[error("Database persistence error: {0}")]
    Persistence(#[from] PersistenceError),
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("EVM error: {0}")]
    Evm(#[from] st0x_evm::EvmError),
    #[error("Sol type error: {0}")]
    SolType(#[from] alloy::sol_types::Error),
    #[error("RPC transport error: {0}")]
    RpcTransport(#[from] RpcError<TransportErrorKind>),
    #[error("Invalid IO index: {0}")]
    InvalidIndex(#[from] FromUintError<usize>),
    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),
    #[error("Order status parse error: {0}")]
    OrderStatusParse(#[from] ParseOrderStatusError),
    #[error("Invalid executor: {0}")]
    InvalidExecutor(#[from] InvalidExecutorError),
    #[error("Float conversion error: {0}")]
    FloatConversion(#[from] FloatError),
    #[error("Integer conversion error: {0}")]
    IntConversion(#[from] TryFromIntError),
    #[error(transparent)]
    EmptySymbol(#[from] EmptySymbolError),
    #[error(transparent)]
    InvalidShares(#[from] InvalidSharesError),
    #[error(transparent)]
    InvalidDirection(#[from] InvalidDirectionError),
    #[error("Position error: {0}")]
    Position(#[from] PositionError),
    #[error("Shares conversion error: {0}")]
    SharesConversion(#[from] SharesConversionError),
    #[error(transparent)]
    NotPositive(#[from] NotPositive<FractionalShares>),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("UUID parse error: {0}")]
    Uuid(#[from] uuid::Error),
    #[error("Position projection error: {0}")]
    PositionProjection(#[from] ProjectionError<Position>),
    #[error("Market hours check failed")]
    MarketHoursCheck(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to push job into queue: {0}")]
    JobQueue(#[from] crate::conductor::job::QueuePushError),
    /// The RPC node answered an `eth_getLogs` for a block range it has
    /// not finished indexing -- the node's reported tip is behind the
    /// requested `to_block`. Returning this error lets the retry loop
    /// reissue the request so a load-balancer routes to a different
    /// upstream node that has caught up.
    #[error(
        "RPC node tip {observed_tip} is behind the requested to_block \
         {required_tip}; the getLogs response cannot be trusted"
    )]
    NodeLaggingBehindRequest {
        observed_tip: u64,
        required_tip: u64,
    },
}

#[cfg(test)]
mod tests {
    //! Mapping tests for [`raindex_contracts`]: each address must land in
    //! its own slot for both inventory modes.

    use alloy::primitives::address;

    use st0x_config::{InventoryMode, TradingChain};

    use super::raindex_contracts;

    /// `inventory` and `orderbook` are both `Address`, so a swap between them
    /// compiles. Under `Managed` it would send rebalancing `deposit4`/
    /// `withdraw4` to the orderbook and orderbook calls to the inventory
    /// contract, so each must be asserted in its own slot.
    #[test]
    fn managed_mode_maps_each_address_to_its_own_slot() {
        let inventory = address!("0x2222222222222222222222222222222222222222");
        let contracts = raindex_contracts(
            &TradingChain::test()
                .required_confirmations(1)
                .inventory(InventoryMode::Managed { inventory })
                .vault_owner(address!("0x3333333333333333333333333333333333333333"))
                .deployment_block(1)
                .call(),
        );

        assert_eq!(contracts.inventory, inventory);
        assert_eq!(
            contracts.orderbook,
            address!("0x1111111111111111111111111111111111111111")
        );
    }

    /// Legacy has no distinct inventory contract, so both slots are the
    /// orderbook and a swap is invisible -- which is why the managed case
    /// above is the one that pins the mapping.
    #[test]
    fn legacy_mode_points_both_slots_at_the_orderbook() {
        let contracts = raindex_contracts(
            &TradingChain::test()
                .required_confirmations(1)
                .inventory(InventoryMode::Legacy)
                .vault_owner(address!("0x3333333333333333333333333333333333333333"))
                .deployment_block(1)
                .call(),
        );

        assert_eq!(contracts.inventory, contracts.orderbook);
        assert_eq!(
            contracts.orderbook,
            address!("0x1111111111111111111111111111111111111111")
        );
    }
}
