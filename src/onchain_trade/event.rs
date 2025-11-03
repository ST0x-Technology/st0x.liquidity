use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, Symbol};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PythPrice {
    pub(crate) value: String,
    pub(crate) expo: i32,
    pub(crate) conf: String,
    pub(crate) publish_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum OnChainTradeEvent {
    Filled {
        symbol: Symbol,
        amount: Decimal,
        direction: Direction,
        price_usdc: Decimal,
        block_number: u64,
        block_timestamp: DateTime<Utc>,
        filled_at: DateTime<Utc>,
    },
    Enriched {
        gas_used: u64,
        pyth_price: PythPrice,
        enriched_at: DateTime<Utc>,
    },
    Genesis {
        symbol: Symbol,
        amount: Decimal,
        direction: Direction,
        price_usdc: Decimal,
        block_number: u64,
        block_timestamp: DateTime<Utc>,
        gas_used: Option<u64>,
        pyth_price: Option<PythPrice>,
        migrated_at: DateTime<Utc>,
    },
}

impl DomainEvent for OnChainTradeEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Filled { .. } => "OnChainTradeEvent::Filled".to_string(),
            Self::Enriched { .. } => "OnChainTradeEvent::Enriched".to_string(),
            Self::Genesis { .. } => "OnChainTradeEvent::Genesis".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
