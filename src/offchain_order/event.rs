use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, SupportedBroker, Symbol};

use crate::position::{BrokerOrderId, FractionalShares, PriceCents};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum MigratedOrderStatus {
    Pending,
    Submitted,
    Filled,
    Failed { error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum OffchainOrderEvent {
    Migrated {
        symbol: Symbol,
        shares: FractionalShares,
        direction: Direction,
        broker: SupportedBroker,
        status: MigratedOrderStatus,
        broker_order_id: Option<BrokerOrderId>,
        price_cents: Option<PriceCents>,
        executed_at: Option<DateTime<Utc>>,
        migrated_at: DateTime<Utc>,
    },
    Placed {
        symbol: Symbol,
        shares: Decimal,
        direction: Direction,
        broker: SupportedBroker,
        placed_at: DateTime<Utc>,
    },
    Submitted {
        broker_order_id: BrokerOrderId,
        submitted_at: DateTime<Utc>,
    },
    PartiallyFilled {
        shares_filled: Decimal,
        avg_price_cents: PriceCents,
        partially_filled_at: DateTime<Utc>,
    },
    Filled {
        price_cents: PriceCents,
        filled_at: DateTime<Utc>,
    },
    Failed {
        error: String,
        failed_at: DateTime<Utc>,
    },
}

impl DomainEvent for OffchainOrderEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Migrated { .. } => "OffchainOrderEvent::Migrated".to_string(),
            Self::Placed { .. } => "OffchainOrderEvent::Placed".to_string(),
            Self::Submitted { .. } => "OffchainOrderEvent::Submitted".to_string(),
            Self::PartiallyFilled { .. } => "OffchainOrderEvent::PartiallyFilled".to_string(),
            Self::Filled { .. } => "OffchainOrderEvent::Filled".to_string(),
            Self::Failed { .. } => "OffchainOrderEvent::Failed".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
