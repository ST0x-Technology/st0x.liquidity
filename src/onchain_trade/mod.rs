use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use tracing::warn;

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

#[derive(Debug, thiserror::Error)]
pub(crate) enum OnChainTradeError {
    #[error("Cannot enrich trade that hasn't been filled yet")]
    NotFilled,
    #[error("Trade has already been enriched")]
    AlreadyEnriched,
    #[error("Trade has already been filled")]
    AlreadyFilled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum OnChainTrade {
    Unfilled,
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
        symbol: Symbol,
        amount: Decimal,
        direction: Direction,
        price_usdc: Decimal,
        block_number: u64,
        block_timestamp: DateTime<Utc>,
        filled_at: DateTime<Utc>,
        gas_used: u64,
        pyth_price: PythPrice,
        enriched_at: DateTime<Utc>,
    },
}

impl Default for OnChainTrade {
    fn default() -> Self {
        Self::Unfilled
    }
}

impl OnChainTrade {
    pub(crate) fn aggregate_id(tx_hash: TxHash, log_index: u64) -> String {
        format!("{tx_hash}:{log_index}")
    }
}

#[async_trait]
impl Aggregate for OnChainTrade {
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
        match command {
            OnChainTradeCommand::Migrate {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
                gas_used,
                pyth_price,
            } => match self {
                Self::Unfilled => {
                    let now = Utc::now();

                    Ok(vec![OnChainTradeEvent::Migrated {
                        symbol,
                        amount,
                        direction,
                        price_usdc,
                        block_number,
                        block_timestamp,
                        gas_used,
                        pyth_price,
                        migrated_at: now,
                    }])
                }
                Self::Filled { .. } | Self::Enriched { .. } => {
                    Err(OnChainTradeError::AlreadyFilled)
                }
            },
            OnChainTradeCommand::Witness {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
            } => match self {
                Self::Unfilled => {
                    let now = Utc::now();

                    Ok(vec![OnChainTradeEvent::Filled {
                        symbol,
                        amount,
                        direction,
                        price_usdc,
                        block_number,
                        block_timestamp,
                        filled_at: now,
                    }])
                }
                Self::Filled { .. } | Self::Enriched { .. } => {
                    Err(OnChainTradeError::AlreadyFilled)
                }
            },

            OnChainTradeCommand::Enrich {
                gas_used,
                pyth_price,
            } => match self {
                Self::Unfilled => Err(OnChainTradeError::NotFilled),
                Self::Enriched { .. } => Err(OnChainTradeError::AlreadyEnriched),
                Self::Filled { .. } => {
                    let now = Utc::now();

                    Ok(vec![OnChainTradeEvent::Enriched {
                        gas_used,
                        pyth_price,
                        enriched_at: now,
                    }])
                }
            },
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            OnChainTradeEvent::Migrated {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
                gas_used,
                pyth_price,
                migrated_at: _,
            } => {
                if let (Some(gas), Some(pyth)) = (gas_used, pyth_price) {
                    *self = Self::Enriched {
                        symbol,
                        amount,
                        direction,
                        price_usdc,
                        block_number,
                        block_timestamp,
                        filled_at: block_timestamp,
                        gas_used: gas,
                        pyth_price: pyth,
                        enriched_at: block_timestamp,
                    };
                } else {
                    *self = Self::Filled {
                        symbol,
                        amount,
                        direction,
                        price_usdc,
                        block_number,
                        block_timestamp,
                        filled_at: block_timestamp,
                    };
                }
            }
            OnChainTradeEvent::Filled {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
                filled_at,
            } => {
                *self = Self::Filled {
                    symbol,
                    amount,
                    direction,
                    price_usdc,
                    block_number,
                    block_timestamp,
                    filled_at,
                };
            }

            OnChainTradeEvent::Enriched {
                gas_used,
                pyth_price,
                enriched_at,
            } => {
                if let Self::Filled {
                    symbol,
                    amount,
                    direction,
                    price_usdc,
                    block_number,
                    block_timestamp,
                    filled_at,
                } = self
                {
                    *self = Self::Enriched {
                        symbol: symbol.clone(),
                        amount: *amount,
                        direction: *direction,
                        price_usdc: *price_usdc,
                        block_number: *block_number,
                        block_timestamp: *block_timestamp,
                        filled_at: *filled_at,
                        gas_used,
                        pyth_price,
                        enriched_at,
                    };
                } else {
                    warn!(
                        current_state = ?self,
                        enriched_at = %enriched_at,
                        "Enriched event applied to non-Filled aggregate - indicates bug in command validation"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::b256;
    use chrono::Utc;
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_aggregate_id() {
        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        let log_index = 42;

        let aggregate_id = OnChainTrade::aggregate_id(tx_hash, log_index);

        assert_eq!(
            aggregate_id,
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef:42"
        );
    }

    #[tokio::test]
    async fn test_witness_command_creates_filled_event() {
        let aggregate = OnChainTrade::default();
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
    async fn test_enrich_command_creates_enriched_event() {
        let mut aggregate = OnChainTrade::default();
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
    async fn test_cannot_enrich_twice() {
        let mut aggregate = OnChainTrade::default();
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
    async fn test_cannot_enrich_before_fill() {
        let aggregate = OnChainTrade::default();
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
    async fn test_migrated_event_with_enrichment() {
        let mut aggregate = OnChainTrade::default();
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

        assert!(matches!(aggregate, OnChainTrade::Enriched { .. }));
    }

    #[tokio::test]
    async fn test_migrated_event_without_enrichment() {
        let mut aggregate = OnChainTrade::default();
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

        assert!(matches!(aggregate, OnChainTrade::Filled { .. }));
    }

    #[tokio::test]
    async fn test_cannot_witness_twice_when_filled() {
        let mut aggregate = OnChainTrade::default();
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
    async fn test_cannot_witness_when_enriched() {
        let mut aggregate = OnChainTrade::default();
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
