//! OnChainTrade CQRS/ES aggregate for recording DEX fills
//! from the Raindex orderbook.
//!
//! Keyed by `(tx_hash, log_index)`. Can be enriched after
//! the fact with gas costs and Pyth oracle price data.

use std::num::ParseIntError;
use std::str::FromStr;

use alloy::hex::FromHexError;
use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use st0x_execution::{Direction, Symbol};

use st0x_event_sorcery::{DomainEvent, EventSourced, Nil};

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

#[derive(Debug, Error)]
pub(crate) enum ParseOnChainTradeIdError {
    #[error("expected 'tx_hash:log_index', got '{id_provided}'")]
    MissingDelimiter { id_provided: String },

    #[error("invalid tx_hash: {0}")]
    TxHash(#[from] FromHexError),

    #[error("invalid log_index: {0}")]
    LogIndex(#[from] ParseIntError),
}

impl FromStr for OnChainTradeId {
    type Err = ParseOnChainTradeIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (tx_hash_str, log_index_str) =
            value
                .split_once(':')
                .ok_or_else(|| ParseOnChainTradeIdError::MissingDelimiter {
                    id_provided: value.to_string(),
                })?;
        let tx_hash = tx_hash_str.parse()?;
        let log_index = log_index_str.parse()?;
        Ok(Self { tx_hash, log_index })
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

#[async_trait]
impl EventSourced for OnChainTrade {
    type Id = OnChainTradeId;
    type Event = OnChainTradeEvent;
    type Command = OnChainTradeCommand;
    type Error = OnChainTradeError;
    type Services = ();
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "OnChainTrade";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        use OnChainTradeEvent::*;
        match event {
            Filled {
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

            Enriched { .. } => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use OnChainTradeEvent::*;
        match event {
            Enriched {
                gas_used,
                pyth_price,
                enriched_at,
            } => Ok(Some(Self {
                enrichment: Some(Enrichment {
                    gas_used: *gas_used,
                    pyth_price: pyth_price.clone(),
                    enriched_at: *enriched_at,
                }),
                ..entity.clone()
            })),

            Filled { .. } => Ok(None),
        }
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use OnChainTradeCommand::*;
        use OnChainTradeEvent::*;
        match command {
            Witness {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
            } => Ok(vec![Filled {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
                filled_at: Utc::now(),
            }]),

            Enrich { .. } => Err(OnChainTradeError::NotFilled),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use OnChainTradeCommand::*;
        use OnChainTradeEvent::*;
        match command {
            Witness { .. } => Err(OnChainTradeError::AlreadyFilled),

            Enrich {
                gas_used,
                pyth_price,
            } => {
                if self.is_enriched() {
                    return Err(OnChainTradeError::AlreadyEnriched);
                }

                Ok(vec![Enriched {
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
    use rust_decimal_macros::dec;

    use st0x_event_sorcery::{LifecycleError, TestHarness, replay};

    use super::*;

    #[tokio::test]
    async fn witness_command_creates_filled_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let events = TestHarness::<OnChainTrade>::with(())
            .given_no_previous_events()
            .when(OnChainTradeCommand::Witness {
                symbol: symbol.clone(),
                amount: dec!(10.5),
                direction: Direction::Buy,
                price_usdc: dec!(150.25),
                block_number: 12345,
                block_timestamp: now,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OnChainTradeEvent::Filled { .. }));
    }

    #[tokio::test]
    async fn enrich_command_creates_enriched_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let events = TestHarness::<OnChainTrade>::with(())
            .given(vec![OnChainTradeEvent::Filled {
                symbol: symbol.clone(),
                amount: dec!(10.5),
                direction: Direction::Buy,
                price_usdc: dec!(150.25),
                block_number: 12345,
                block_timestamp: now,
                filled_at: now,
            }])
            .when(OnChainTradeCommand::Enrich {
                gas_used: 50000,
                pyth_price,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OnChainTradeEvent::Enriched { .. }));
    }

    #[tokio::test]
    async fn cannot_enrich_twice() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let error = TestHarness::<OnChainTrade>::with(())
            .given(vec![
                OnChainTradeEvent::Filled {
                    symbol: symbol.clone(),
                    amount: dec!(10.5),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.25),
                    block_number: 12345,
                    block_timestamp: now,
                    filled_at: now,
                },
                OnChainTradeEvent::Enriched {
                    gas_used: 50000,
                    pyth_price: pyth_price.clone(),
                    enriched_at: now,
                },
            ])
            .when(OnChainTradeCommand::Enrich {
                gas_used: 50000,
                pyth_price,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::AlreadyEnriched)
        ));
    }

    #[tokio::test]
    async fn cannot_enrich_before_fill() {
        let now = Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let error = TestHarness::<OnChainTrade>::with(())
            .given_no_previous_events()
            .when(OnChainTradeCommand::Enrich {
                gas_used: 50000,
                pyth_price,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::NotFilled)
        ));
    }

    #[tokio::test]
    async fn cannot_witness_twice_when_filled() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let error = TestHarness::<OnChainTrade>::with(())
            .given(vec![OnChainTradeEvent::Filled {
                symbol: symbol.clone(),
                amount: dec!(10.5),
                direction: Direction::Buy,
                price_usdc: dec!(150.25),
                block_number: 12345,
                block_timestamp: now,
                filled_at: now,
            }])
            .when(OnChainTradeCommand::Witness {
                symbol: symbol.clone(),
                amount: dec!(10.5),
                direction: Direction::Buy,
                price_usdc: dec!(150.25),
                block_number: 12345,
                block_timestamp: now,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::AlreadyFilled)
        ));
    }

    #[tokio::test]
    async fn cannot_witness_when_enriched() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let error = TestHarness::<OnChainTrade>::with(())
            .given(vec![
                OnChainTradeEvent::Filled {
                    symbol: symbol.clone(),
                    amount: dec!(10.5),
                    direction: Direction::Buy,
                    price_usdc: dec!(150.25),
                    block_number: 12345,
                    block_timestamp: now,
                    filled_at: now,
                },
                OnChainTradeEvent::Enriched {
                    gas_used: 50000,
                    pyth_price,
                    enriched_at: now,
                },
            ])
            .when(OnChainTradeCommand::Witness {
                symbol: symbol.clone(),
                amount: dec!(10.5),
                direction: Direction::Buy,
                price_usdc: dec!(150.25),
                block_number: 12345,
                block_timestamp: now,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::AlreadyFilled)
        ));
    }

    #[test]
    fn filled_creates_live_state() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let trade = replay::<OnChainTrade>(vec![OnChainTradeEvent::Filled {
            symbol,
            amount: dec!(10.5),
            direction: Direction::Buy,
            price_usdc: dec!(150.25),
            block_number: 12345,
            block_timestamp: now,
            filled_at: now,
        }])
        .unwrap()
        .unwrap();

        assert_eq!(trade.symbol, Symbol::new("AAPL").unwrap());
        assert_eq!(trade.amount, dec!(10.5));
        assert_eq!(trade.direction, Direction::Buy);
        assert!(!trade.is_enriched());
    }

    #[test]
    fn enriched_updates_live_state() {
        let now = Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let trade = replay::<OnChainTrade>(vec![
            OnChainTradeEvent::Filled {
                symbol: Symbol::new("AAPL").unwrap(),
                amount: dec!(10.5),
                direction: Direction::Buy,
                price_usdc: dec!(150.25),
                block_number: 12345,
                block_timestamp: now,
                filled_at: now,
            },
            OnChainTradeEvent::Enriched {
                gas_used: 50000,
                pyth_price: pyth_price.clone(),
                enriched_at: now,
            },
        ])
        .unwrap()
        .unwrap();

        assert!(trade.is_enriched());
        let enrichment = trade.enrichment.unwrap();
        assert_eq!(enrichment.gas_used, 50000);
        assert_eq!(enrichment.pyth_price, pyth_price);
    }

    #[test]
    fn transition_on_uninitialized_fails() {
        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: Utc::now(),
        };

        let error = replay::<OnChainTrade>(vec![OnChainTradeEvent::Enriched {
            gas_used: 50000,
            pyth_price,
            enriched_at: Utc::now(),
        }])
        .unwrap_err();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
    }
}
