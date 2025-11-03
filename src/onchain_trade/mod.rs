mod cmd;
mod event;
pub(crate) mod view;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::{Direction, Symbol};

pub(crate) use cmd::OnChainTradeCommand;
pub(crate) use event::{OnChainTradeEvent, PythPrice};

#[derive(Debug, thiserror::Error)]
pub(crate) enum OnChainTradeError {
    #[error("Cannot enrich trade that hasn't been filled yet")]
    NotFilled,
    #[error("Trade has already been enriched")]
    AlreadyEnriched,
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
            OnChainTradeCommand::Witness {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
            } => {
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
                }
            }
            OnChainTradeEvent::Genesis {
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

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
    async fn test_genesis_event_with_enrichment() {
        let mut aggregate = OnChainTrade::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let genesis_event = OnChainTradeEvent::Genesis {
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

        aggregate.apply(genesis_event);

        assert!(matches!(aggregate, OnChainTrade::Enriched { .. }));
    }

    #[tokio::test]
    async fn test_genesis_event_without_enrichment() {
        let mut aggregate = OnChainTrade::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let genesis_event = OnChainTradeEvent::Genesis {
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

        aggregate.apply(genesis_event);

        assert!(matches!(aggregate, OnChainTrade::Filled { .. }));
    }
}
