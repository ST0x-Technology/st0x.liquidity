use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{Aggregate, DomainEvent};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

use crate::lifecycle::{Lifecycle, LifecycleError, Never};

mod cmd;
mod event;
pub(crate) mod view;

pub(crate) use cmd::OnChainTradeCommand;
pub(crate) use event::{OnChainTradeEvent, PythPrice};
use st0x_broker::{Direction, Symbol};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TradeAggregateId {
    pub(crate) tx_hash: TxHash,
    pub(crate) log_index: u64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AggregateIdError {
    #[error("Invalid format: expected 'tx_hash:log_index', got '{0}'")]
    InvalidFormat(String),
    #[error("Failed to parse tx_hash: {0}")]
    ParseTxHash(#[from] alloy::hex::FromHexError),
    #[error("Failed to parse log_index: {0}")]
    ParseLogIndex(#[from] std::num::ParseIntError),
}

impl FromStr for TradeAggregateId {
    type Err = AggregateIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(AggregateIdError::InvalidFormat(s.to_string()));
        }

        let tx_hash = TxHash::from_str(parts[0])?;
        let log_index = parts[1].parse::<u64>()?;

        Ok(Self { tx_hash, log_index })
    }
}

impl Display for TradeAggregateId {
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
    pub(crate) block_number: u64,
    pub(crate) block_timestamp: DateTime<Utc>,
    pub(crate) filled_at: DateTime<Utc>,
    pub(crate) enrichment: Option<Enrichment>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Enrichment {
    pub(crate) gas_used: u64,
    pub(crate) pyth_price: PythPrice,
    pub(crate) enriched_at: DateTime<Utc>,
}

impl OnChainTrade {
    fn is_enriched(&self) -> bool {
        self.enrichment.is_some()
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

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match (&command, self.live()) {
            (
                OnChainTradeCommand::Witness {
                    symbol,
                    amount,
                    direction,
                    price_usdc,
                    block_number,
                    block_timestamp,
                },
                Err(LifecycleError::Uninitialized),
            ) => Ok(vec![OnChainTradeEvent::Filled {
                symbol: symbol.clone(),
                amount: *amount,
                direction: *direction,
                price_usdc: *price_usdc,
                block_number: *block_number,
                block_timestamp: *block_timestamp,
                filled_at: Utc::now(),
            }]),

            (OnChainTradeCommand::Witness { .. }, Ok(_)) => Err(OnChainTradeError::AlreadyFilled),

            (
                OnChainTradeCommand::Enrich {
                    gas_used,
                    pyth_price,
                },
                Ok(trade),
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

            (OnChainTradeCommand::Enrich { .. }, Err(LifecycleError::Uninitialized)) => {
                Err(OnChainTradeError::NotFilled)
            }

            (_, Err(e)) => Err(e.into()),
        }
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, OnChainTrade::apply_transition)
            .or_initialize(&event, OnChainTrade::from_event);
    }
}

impl OnChainTrade {
    fn apply_transition(
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

    fn from_event(event: &OnChainTradeEvent) -> Result<Self, LifecycleError<Never>> {
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
                block_number: *block_number,
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

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

        let result = aggregate.handle(command, &()).await;

        assert!(matches!(result, Err(OnChainTradeError::AlreadyEnriched)));
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

        let result = aggregate.handle(command, &()).await;

        assert!(matches!(result, Err(OnChainTradeError::NotFilled)));
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
            block_number: 12345,
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
            block_number: 12345,
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

        let result = aggregate.handle(command, &()).await;

        assert!(matches!(result, Err(OnChainTradeError::AlreadyFilled)));
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

        let result = aggregate.handle(command, &()).await;

        assert!(matches!(result, Err(OnChainTradeError::AlreadyFilled)));
    }

    proptest! {
        #[test]
        fn test_aggregate_id_roundtrip(
            tx_hash_bytes in prop::array::uniform32(any::<u8>()),
            log_index in any::<u64>()
        ) {
            let tx_hash = TxHash::from(tx_hash_bytes);
            let aggregate_id = TradeAggregateId { tx_hash, log_index };

            let serialized = aggregate_id.to_string();
            let deserialized = serialized.parse::<TradeAggregateId>().unwrap();

            prop_assert_eq!(aggregate_id, deserialized);
        }
    }

    #[test]
    fn test_aggregate_id_parse_invalid_format() {
        let input = "invalid_format";
        let result = input.parse::<TradeAggregateId>();

        assert!(matches!(
            result.unwrap_err(),
            AggregateIdError::InvalidFormat(_)
        ));
    }

    #[test]
    fn test_aggregate_id_parse_invalid_tx_hash() {
        let input = "not_a_hex_hash:123";
        let result = input.parse::<TradeAggregateId>();

        assert!(matches!(
            result.unwrap_err(),
            AggregateIdError::ParseTxHash(_)
        ));
    }

    #[test]
    fn test_aggregate_id_parse_invalid_log_index() {
        let input =
            "0x1234567890123456789012345678901234567890123456789012345678901234:not_a_number";
        let result = input.parse::<TradeAggregateId>();

        assert!(matches!(
            result.unwrap_err(),
            AggregateIdError::ParseLogIndex(_)
        ));
    }
}
