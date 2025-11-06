use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, SupportedBroker, Symbol};

use crate::position::{BrokerOrderId, PriceCents};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum OffchainOrderCommand {
    Place {
        symbol: Symbol,
        shares: Decimal,
        direction: Direction,
        broker: SupportedBroker,
    },
    ConfirmSubmission {
        broker_order_id: BrokerOrderId,
    },
    UpdatePartialFill {
        shares_filled: Decimal,
        avg_price_cents: PriceCents,
    },
    CompleteFill {
        price_cents: PriceCents,
    },
    MarkFailed {
        error: String,
    },
}
