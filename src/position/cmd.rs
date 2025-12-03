use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, SupportedBroker, Symbol};

use super::event::{
    BrokerOrderId, ExecutionId, ExecutionThreshold, FractionalShares, PriceCents, TradeId,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum PositionCommand {
    Initialize {
        symbol: Symbol,
        threshold: ExecutionThreshold,
    },
    AcknowledgeOnChainFill {
        trade_id: TradeId,
        amount: FractionalShares,
        direction: Direction,
        price_usdc: Decimal,
        block_timestamp: DateTime<Utc>,
    },
    PlaceOffChainOrder {
        execution_id: ExecutionId,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
    },
    CompleteOffChainOrder {
        execution_id: ExecutionId,
        shares_filled: FractionalShares,
        direction: Direction,
        broker_order_id: BrokerOrderId,
        price_cents: PriceCents,
        broker_timestamp: DateTime<Utc>,
    },
    FailOffChainOrder {
        execution_id: ExecutionId,
        error: String,
    },
    UpdateThreshold {
        threshold: ExecutionThreshold,
    },
}
