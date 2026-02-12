//! OnChainTrade CQRS/ES aggregate for recording DEX fills
//! from the Raindex orderbook.
//!
//! Keyed by `(tx_hash, log_index)`. Can be enriched after
//! the fact with gas costs and Pyth oracle price data.

use alloy::primitives::TxHash;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use st0x_execution::{Direction, Symbol};

use crate::event_sourced::EventSourced;
use crate::lifecycle::Lifecycle;

pub(crate) type OnChainTradeCqrs = sqlite_es::SqliteCqrs<Lifecycle<OnChainTrade>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OnChainTradeId {
    pub(crate) tx_hash: TxHash,
    pub(crate) log_index: u64,
}

impl std::fmt::Display for OnChainTradeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.tx_hash, self.log_index)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct OnChainTrade {
    pub(crate) symbol: Symbol,
    pub(crate) amount: Decimal,
    pub(crate) direction: Direction,
    pub(crate) price_usdc: Decimal,
    pub(crate) block_number: Option<u64>,
    pub(crate) block_timestamp: DateTime<Utc>,
    pub(crate) filled_at: DateTime<Utc>,
    pub(crate) enrichment: Option<Enrichment>,
}

impl EventSourced for OnChainTrade {
    type Id = OnChainTradeId;
    type Event = OnChainTradeEvent;
    type Command = OnChainTradeCommand;
    type Error = OnChainTradeError;
    type Services = ();

    const AGGREGATE_TYPE: &'static str = "OnChainTrade";
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            OnChainTradeEvent::Filled {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
                filled_at,
            } => Some(Self {
                symbol: symbol.clone(),
                amount: *amount,
                direction: *direction,
                price_usdc: *price_usdc,
                block_number: Some(*block_number),
                block_timestamp: *block_timestamp,
                filled_at: *filled_at,
                enrichment: None,
            }),

            OnChainTradeEvent::Enriched { .. } => None,
        }
    }

    fn evolve(event: &Self::Event, state: &Self) -> Result<Option<Self>, Self::Error> {
        match event {
            OnChainTradeEvent::Enriched {
                gas_used,
                pyth_price,
                enriched_at,
            } => Ok(Some(Self {
                enrichment: Some(Enrichment {
                    gas_used: *gas_used,
                    pyth_price: pyth_price.clone(),
                    enriched_at: *enriched_at,
                }),
                ..state.clone()
            })),

            OnChainTradeEvent::Filled { .. } => Ok(None),
        }
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            OnChainTradeCommand::Witness {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
            } => Ok(vec![OnChainTradeEvent::Filled {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
                filled_at: Utc::now(),
            }]),

            OnChainTradeCommand::Enrich { .. } => Err(OnChainTradeError::NotFilled),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            OnChainTradeCommand::Witness { .. } => Err(OnChainTradeError::AlreadyFilled),

            OnChainTradeCommand::Enrich {
                gas_used,
                pyth_price,
            } => {
                if self.is_enriched() {
                    return Err(OnChainTradeError::AlreadyEnriched);
                }

                Ok(vec![OnChainTradeEvent::Enriched {
                    gas_used,
                    pyth_price,
                    enriched_at: Utc::now(),
                }])
            }
        }
    }
}

impl OnChainTrade {
    pub(crate) fn is_enriched(&self) -> bool {
        self.enrichment.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub(crate) enum OnChainTradeError {
    #[error("Cannot enrich trade that hasn't been filled yet")]
    NotFilled,
    #[error("Trade has already been enriched")]
    AlreadyEnriched,
    #[error("Trade has already been filled")]
    AlreadyFilled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum OnChainTradeCommand {
    Witness {
        symbol: Symbol,
        amount: Decimal,
        direction: Direction,
        price_usdc: Decimal,
        block_number: u64,
        block_timestamp: DateTime<Utc>,
    },
    Enrich {
        gas_used: u64,
        pyth_price: PythPrice,
    },
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
}

impl DomainEvent for OnChainTradeEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Filled { .. } => "OnChainTradeEvent::Filled".to_string(),
            Self::Enriched { .. } => "OnChainTradeEvent::Enriched".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Enrichment {
    pub(crate) gas_used: u64,
    pub(crate) pyth_price: PythPrice,
    pub(crate) enriched_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PythPrice {
    pub(crate) value: String,
    pub(crate) expo: i32,
    pub(crate) conf: String,
    pub(crate) publish_time: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use cqrs_es::{Aggregate, EventEnvelope, View};
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    use super::*;
    use crate::lifecycle::LifecycleError;

    fn make_envelope(
        aggregate_id: &str,
        sequence: usize,
        event: OnChainTradeEvent,
    ) -> EventEnvelope<Lifecycle<OnChainTrade>> {
        EventEnvelope {
            aggregate_id: aggregate_id.to_string(),
            sequence,
            payload: event,
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn witness_command_creates_filled_event() {
        let aggregate = Lifecycle::<OnChainTrade>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let command = OnChainTradeCommand::Witness {
            symbol: symbol.clone(),
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: 12345,
            block_timestamp: now,
        };

        let events = aggregate.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OnChainTradeEvent::Filled { .. }));
    }

    #[tokio::test]
    async fn enrich_command_creates_enriched_event() {
        let mut aggregate = Lifecycle::<OnChainTrade>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let filled_event = OnChainTradeEvent::Filled {
            symbol: symbol.clone(),
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: 12345,
            block_timestamp: now,
            filled_at: now,
        };
        aggregate.apply(filled_event);

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let command = OnChainTradeCommand::Enrich {
            gas_used: 50000,
            pyth_price,
        };

        let events = aggregate.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OnChainTradeEvent::Enriched { .. }));
    }

    #[tokio::test]
    async fn cannot_enrich_twice() {
        let mut aggregate = Lifecycle::<OnChainTrade>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let filled_event = OnChainTradeEvent::Filled {
            symbol: symbol.clone(),
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: 12345,
            block_timestamp: now,
            filled_at: now,
        };
        aggregate.apply(filled_event);

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let enriched_event = OnChainTradeEvent::Enriched {
            gas_used: 50000,
            pyth_price: pyth_price.clone(),
            enriched_at: now,
        };
        aggregate.apply(enriched_event);

        let command = OnChainTradeCommand::Enrich {
            gas_used: 50000,
            pyth_price,
        };

        assert!(matches!(
            aggregate.handle(command, &()).await,
            Err(LifecycleError::Apply(OnChainTradeError::AlreadyEnriched))
        ));
    }

    #[tokio::test]
    async fn cannot_enrich_before_fill() {
        let aggregate = Lifecycle::<OnChainTrade>::default();
        let now = Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let command = OnChainTradeCommand::Enrich {
            gas_used: 50000,
            pyth_price,
        };

        assert!(matches!(
            aggregate.handle(command, &()).await,
            Err(LifecycleError::Apply(OnChainTradeError::NotFilled))
        ));
    }

    #[tokio::test]
    async fn cannot_witness_twice_when_filled() {
        let mut aggregate = Lifecycle::<OnChainTrade>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let filled_event = OnChainTradeEvent::Filled {
            symbol: symbol.clone(),
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: 12345,
            block_timestamp: now,
            filled_at: now,
        };
        aggregate.apply(filled_event);

        let command = OnChainTradeCommand::Witness {
            symbol: symbol.clone(),
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: 12345,
            block_timestamp: now,
        };

        assert!(matches!(
            aggregate.handle(command, &()).await,
            Err(LifecycleError::Apply(OnChainTradeError::AlreadyFilled))
        ));
    }

    #[tokio::test]
    async fn cannot_witness_when_enriched() {
        let mut aggregate = Lifecycle::<OnChainTrade>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let filled_event = OnChainTradeEvent::Filled {
            symbol: symbol.clone(),
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: 12345,
            block_timestamp: now,
            filled_at: now,
        };
        aggregate.apply(filled_event);

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let enriched_event = OnChainTradeEvent::Enriched {
            gas_used: 50000,
            pyth_price,
            enriched_at: now,
        };
        aggregate.apply(enriched_event);

        let command = OnChainTradeCommand::Witness {
            symbol: symbol.clone(),
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: 12345,
            block_timestamp: now,
        };

        assert!(matches!(
            aggregate.handle(command, &()).await,
            Err(LifecycleError::Apply(OnChainTradeError::AlreadyFilled))
        ));
    }

    #[test]
    fn filled_creates_live_state() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let event = OnChainTradeEvent::Filled {
            symbol,
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: 12345,
            block_timestamp: now,
            filled_at: now,
        };

        let mut view = Lifecycle::<OnChainTrade>::default();
        assert!(matches!(view, Lifecycle::Uninitialized));

        view.update(&make_envelope("0x1234:0", 1, event));

        let Lifecycle::Live(trade) = view else {
            panic!("Expected Live state");
        };

        assert_eq!(trade.symbol, Symbol::new("AAPL").unwrap());
        assert_eq!(trade.amount, dec!(10.5));
        assert_eq!(trade.direction, Direction::Buy);
        assert!(!trade.is_enriched());
    }

    #[test]
    fn enriched_updates_live_state() {
        let now = Utc::now();

        let mut view = Lifecycle::Live(OnChainTrade {
            symbol: Symbol::new("AAPL").unwrap(),
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: Some(12345),
            block_timestamp: now,
            filled_at: now,
            enrichment: None,
        });

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let event = OnChainTradeEvent::Enriched {
            gas_used: 50000,
            pyth_price: pyth_price.clone(),
            enriched_at: now,
        };

        view.update(&make_envelope("0x1234:0", 2, event));

        let Lifecycle::Live(trade) = view else {
            panic!("Expected Live state");
        };

        assert!(trade.is_enriched());
        let enrichment = trade.enrichment.unwrap();
        assert_eq!(enrichment.gas_used, 50000);
        assert_eq!(enrichment.pyth_price, pyth_price);
    }

    #[test]
    fn transition_on_uninitialized_fails() {
        let mut view = Lifecycle::<OnChainTrade>::default();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: Utc::now(),
        };

        let event = OnChainTradeEvent::Enriched {
            gas_used: 50000,
            pyth_price,
            enriched_at: Utc::now(),
        };

        view.update(&make_envelope("0x1234:0", 1, event));

        assert!(matches!(view, Lifecycle::Failed { .. }));
    }
}
