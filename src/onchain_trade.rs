//! OnChainTrade CQRS/ES aggregate for recording DEX fills
//! from the Raindex orderbook.
//!
//! Keyed by `(tx_hash, log_index)`. Can be enriched after
//! the fact with gas costs and Pyth oracle price data.

use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{Aggregate, DomainEvent};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_execution::{Direction, Symbol};

use crate::lifecycle::{Lifecycle, LifecycleError, Never};

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

impl OnChainTrade {
    pub(crate) fn aggregate_id(tx_hash: TxHash, log_index: u64) -> String {
        format!("{tx_hash}:{log_index}")
    }

    pub(crate) fn is_enriched(&self) -> bool {
        self.enrichment.is_some()
    }

    pub(crate) fn apply_transition(
        event: &OnChainTradeEvent,
        trade: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        match event {
            OnChainTradeEvent::Enriched {
                gas_used,
                pyth_price,
                enriched_at,
            } => Ok(Self {
                enrichment: Some(Enrichment {
                    gas_used: *gas_used,
                    pyth_price: pyth_price.clone(),
                    enriched_at: *enriched_at,
                }),
                ..trade.clone()
            }),

            OnChainTradeEvent::Filled { .. } | OnChainTradeEvent::Migrated { .. } => {
                Err(LifecycleError::Mismatch {
                    state: format!("{trade:?}"),
                    event: event.event_type(),
                })
            }
        }
    }

    pub(crate) fn from_event(event: &OnChainTradeEvent) -> Result<Self, LifecycleError<Never>> {
        match event {
            OnChainTradeEvent::Filled {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
                filled_at,
            } => Ok(Self {
                symbol: symbol.clone(),
                amount: *amount,
                direction: *direction,
                price_usdc: *price_usdc,
                block_number: Some(*block_number),
                block_timestamp: *block_timestamp,
                filled_at: *filled_at,
                enrichment: None,
            }),

            OnChainTradeEvent::Migrated {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
                gas_used,
                pyth_price,
                migrated_at,
            } => {
                let enrichment = gas_used
                    .zip(pyth_price.clone())
                    .map(|(gas, pyth)| Enrichment {
                        gas_used: gas,
                        pyth_price: pyth,
                        enriched_at: *migrated_at,
                    });

                Ok(Self {
                    symbol: symbol.clone(),
                    amount: *amount,
                    direction: *direction,
                    price_usdc: *price_usdc,
                    block_number: *block_number,
                    block_timestamp: *block_timestamp,
                    filled_at: *block_timestamp,
                    enrichment,
                })
            }

            OnChainTradeEvent::Enriched { .. } => Err(LifecycleError::Mismatch {
                state: "Uninitialized".into(),
                event: event.event_type(),
            }),
        }
    }
}

#[async_trait]
impl Aggregate for Lifecycle<OnChainTrade, Never> {
    type Command = OnChainTradeCommand;
    type Event = OnChainTradeEvent;
    type Error = OnChainTradeError;
    type Services = ();

    fn aggregate_type() -> String {
        "OnChainTrade".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, OnChainTrade::apply_transition)
            .or_initialize(&event, OnChainTrade::from_event);
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match (self.live(), &command) {
            (
                Err(LifecycleError::Uninitialized),
                OnChainTradeCommand::Migrate {
                    symbol,
                    amount,
                    direction,
                    price_usdc,
                    block_number,
                    block_timestamp,
                    gas_used,
                    pyth_price,
                },
            ) => Ok(vec![OnChainTradeEvent::Migrated {
                symbol: symbol.clone(),
                amount: *amount,
                direction: *direction,
                price_usdc: *price_usdc,
                block_number: *block_number,
                block_timestamp: *block_timestamp,
                gas_used: *gas_used,
                pyth_price: pyth_price.clone(),
                migrated_at: Utc::now(),
            }]),

            (
                Err(LifecycleError::Uninitialized),
                OnChainTradeCommand::Witness {
                    symbol,
                    amount,
                    direction,
                    price_usdc,
                    block_number,
                    block_timestamp,
                },
            ) => Ok(vec![OnChainTradeEvent::Filled {
                symbol: symbol.clone(),
                amount: *amount,
                direction: *direction,
                price_usdc: *price_usdc,
                block_number: *block_number,
                block_timestamp: *block_timestamp,
                filled_at: Utc::now(),
            }]),

            (Ok(_), OnChainTradeCommand::Migrate { .. } | OnChainTradeCommand::Witness { .. }) => {
                Err(OnChainTradeError::AlreadyFilled)
            }

            (
                Ok(trade),
                OnChainTradeCommand::Enrich {
                    gas_used,
                    pyth_price,
                },
            ) => {
                if trade.is_enriched() {
                    return Err(OnChainTradeError::AlreadyEnriched);
                }

                Ok(vec![OnChainTradeEvent::Enriched {
                    gas_used: *gas_used,
                    pyth_price: pyth_price.clone(),
                    enriched_at: Utc::now(),
                }])
            }

            (Err(LifecycleError::Uninitialized), OnChainTradeCommand::Enrich { .. }) => {
                Err(OnChainTradeError::NotFilled)
            }

            (Err(e), _) => Err(e.into()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OnChainTradeError {
    #[error("Cannot enrich trade that hasn't been filled yet")]
    NotFilled,
    #[error("Trade has already been enriched")]
    AlreadyEnriched,
    #[error("Trade has already been filled")]
    AlreadyFilled,
    #[error(transparent)]
    State(#[from] LifecycleError<Never>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum OnChainTradeCommand {
    Migrate {
        symbol: Symbol,
        amount: Decimal,
        direction: Direction,
        price_usdc: Decimal,
        block_number: Option<u64>,
        block_timestamp: DateTime<Utc>,
        gas_used: Option<u64>,
        pyth_price: Option<PythPrice>,
    },
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
    Migrated {
        symbol: Symbol,
        amount: Decimal,
        direction: Direction,
        price_usdc: Decimal,
        block_number: Option<u64>,
        block_timestamp: DateTime<Utc>,
        gas_used: Option<u64>,
        pyth_price: Option<PythPrice>,
        migrated_at: DateTime<Utc>,
    },
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
            Self::Migrated { .. } => "OnChainTradeEvent::Migrated".to_string(),
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
    use cqrs_es::EventEnvelope;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    use super::*;

    fn make_envelope(
        aggregate_id: &str,
        sequence: usize,
        event: OnChainTradeEvent,
    ) -> EventEnvelope<Lifecycle<OnChainTrade, Never>> {
        EventEnvelope {
            aggregate_id: aggregate_id.to_string(),
            sequence,
            payload: event,
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn witness_command_creates_filled_event() {
        let aggregate = Lifecycle::<OnChainTrade, Never>::default();
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
        let mut aggregate = Lifecycle::<OnChainTrade, Never>::default();
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
        let mut aggregate = Lifecycle::<OnChainTrade, Never>::default();
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
            Err(OnChainTradeError::AlreadyEnriched)
        ));
    }

    #[tokio::test]
    async fn cannot_enrich_before_fill() {
        let aggregate = Lifecycle::<OnChainTrade, Never>::default();
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
            Err(OnChainTradeError::NotFilled)
        ));
    }

    #[tokio::test]
    async fn migrated_event_with_enrichment() {
        let mut aggregate = Lifecycle::<OnChainTrade, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let migrated_event = OnChainTradeEvent::Migrated {
            symbol,
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: Some(12345),
            block_timestamp: now,
            gas_used: Some(50000),
            pyth_price: Some(pyth_price),
            migrated_at: now,
        };

        aggregate.apply(migrated_event);

        let Lifecycle::Live(trade) = aggregate else {
            panic!("Expected Active state");
        };
        assert!(trade.is_enriched());
    }

    #[tokio::test]
    async fn migrated_event_without_enrichment() {
        let mut aggregate = Lifecycle::<OnChainTrade, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let migrated_event = OnChainTradeEvent::Migrated {
            symbol,
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: Some(12345),
            block_timestamp: now,
            gas_used: None,
            pyth_price: None,
            migrated_at: now,
        };

        aggregate.apply(migrated_event);

        let Lifecycle::Live(trade) = aggregate else {
            panic!("Expected Active state");
        };
        assert!(!trade.is_enriched());
    }

    #[tokio::test]
    async fn cannot_witness_twice_when_filled() {
        let mut aggregate = Lifecycle::<OnChainTrade, Never>::default();
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
            Err(OnChainTradeError::AlreadyFilled)
        ));
    }

    #[tokio::test]
    async fn cannot_witness_when_enriched() {
        let mut aggregate = Lifecycle::<OnChainTrade, Never>::default();
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
            Err(OnChainTradeError::AlreadyFilled)
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

        let mut view = Lifecycle::<OnChainTrade, Never>::default();
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
    fn migrated_creates_live_state_with_enrichment() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let event = OnChainTradeEvent::Migrated {
            symbol,
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: Some(12345),
            block_timestamp: now,
            gas_used: Some(50000),
            pyth_price: Some(pyth_price),
            migrated_at: now,
        };

        let mut view = Lifecycle::<OnChainTrade, Never>::default();
        view.update(&make_envelope("0x1234:0", 1, event));

        let Lifecycle::Live(trade) = view else {
            panic!("Expected Live state");
        };

        assert_eq!(trade.symbol, Symbol::new("AAPL").unwrap());
        assert!(trade.is_enriched());
        let enrichment = trade.enrichment.unwrap();
        assert_eq!(enrichment.gas_used, 50000);
    }

    #[test]
    fn migrated_creates_live_state_without_enrichment() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let event = OnChainTradeEvent::Migrated {
            symbol: symbol.clone(),
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: Some(12345),
            block_timestamp: now,
            gas_used: None,
            pyth_price: None,
            migrated_at: now,
        };

        let mut view = Lifecycle::<OnChainTrade, Never>::default();
        view.update(&make_envelope("0x1234:0", 1, event));

        let Lifecycle::Live(trade) = view else {
            panic!("Expected Live state");
        };

        assert_eq!(trade.symbol, symbol);
        assert!(!trade.is_enriched());
    }

    #[test]
    fn transition_on_uninitialized_fails() {
        let mut view = Lifecycle::<OnChainTrade, Never>::default();

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

    #[tokio::test]
    async fn test_migrate_command_creates_migrated_event() {
        let aggregate = Lifecycle::<OnChainTrade, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let command = OnChainTradeCommand::Migrate {
            symbol: symbol.clone(),
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: Some(12345),
            block_timestamp: now,
            gas_used: None,
            pyth_price: None,
        };

        let events = aggregate.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            OnChainTradeEvent::Migrated {
                symbol: evt_symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                gas_used,
                pyth_price,
                ..
            } => {
                assert_eq!(evt_symbol, &symbol);
                assert_eq!(amount, &dec!(10.5));
                assert_eq!(direction, &Direction::Buy);
                assert_eq!(price_usdc, &dec!(150.25));
                assert_eq!(block_number, &Some(12345));
                assert!(gas_used.is_none());
                assert!(pyth_price.is_none());
            }
            _ => panic!("Expected Migrated event"),
        }
    }

    #[tokio::test]
    async fn test_migrate_command_with_enrichment() {
        let aggregate = Lifecycle::<OnChainTrade, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let command = OnChainTradeCommand::Migrate {
            symbol: symbol.clone(),
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: Some(12345),
            block_timestamp: now,
            gas_used: Some(50000),
            pyth_price: Some(pyth_price.clone()),
        };

        let events = aggregate.handle(command, &()).await.unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            OnChainTradeEvent::Migrated {
                gas_used,
                pyth_price: evt_pyth,
                ..
            } => {
                assert_eq!(gas_used, &Some(50000));
                assert_eq!(evt_pyth, &Some(pyth_price));
            }
            _ => panic!("Expected Migrated event"),
        }
    }

    #[tokio::test]
    async fn test_cannot_migrate_when_already_filled() {
        let mut aggregate = Lifecycle::<OnChainTrade, Never>::default();
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

        let command = OnChainTradeCommand::Migrate {
            symbol,
            amount: dec!(5.0),
            direction: Direction::Sell,
            price_usdc: dec!(160.00),
            block_number: Some(12346),
            block_timestamp: now,
            gas_used: None,
            pyth_price: None,
        };

        assert!(matches!(
            aggregate.handle(command, &()).await,
            Err(OnChainTradeError::AlreadyFilled)
        ));
    }
}
