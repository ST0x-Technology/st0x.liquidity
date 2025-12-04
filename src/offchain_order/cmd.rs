use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, SupportedBroker, Symbol};

use crate::shares::FractionalShares;

use super::{BrokerOrderId, PriceCents};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum OffchainOrderCommand {
    Place {
        symbol: Symbol,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
    },
    ConfirmSubmission {
        broker_order_id: BrokerOrderId,
    },
    UpdatePartialFill {
        shares_filled: FractionalShares,
        avg_price_cents: PriceCents,
    },
    CompleteFill {
        price_cents: PriceCents,
    },
    MarkFailed {
        error: String,
    },
}
