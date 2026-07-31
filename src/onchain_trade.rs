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
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use st0x_dto::{Direction, Trade, TradeOutcome, TradingVenue};
use st0x_event_sorcery::{DomainEvent, EventSourced, Nil};
use st0x_execution::Symbol;
use st0x_finance::FractionalShares;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OnChainTrade {
    pub(crate) symbol: Symbol,
    #[serde(
        serialize_with = "st0x_float_serde::serialize_float_as_string",
        deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
    )]
    pub(crate) amount: Float,
    pub(crate) direction: Direction,
    #[serde(
        serialize_with = "st0x_float_serde::serialize_float_as_string",
        deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
    )]
    pub(crate) price_usdc: Float,
    pub(crate) block_number: Option<u64>,
    pub(crate) block_timestamp: DateTime<Utc>,
    pub(crate) filled_at: DateTime<Utc>,
    pub(crate) enrichment: Option<Enrichment>,
    /// Set once the `Position` aggregate has acknowledged this fill.
    /// The trade-accounting dedupe treats only acknowledged trades as
    /// fully processed, so a job re-delivered after a crash between the
    /// witness and acknowledge writes resumes instead of skipping
    /// (ADR 0005). Absent on aggregates persisted before the marker
    /// existed, which is the resume-safe default.
    #[serde(default)]
    pub(crate) acknowledged_at: Option<DateTime<Utc>>,
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
    const SCHEMA_VERSION: u64 = 2;

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
                acknowledged_at: None,
            }),

            Enriched { .. } | Acknowledged { .. } => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use OnChainTradeEvent::*;
        match event {
            Enriched {
                gas_used,
                effective_gas_price,
                pyth_price,
                enriched_at,
            } => Ok(Some(Self {
                enrichment: Some(Enrichment {
                    gas_used: *gas_used,
                    effective_gas_price: *effective_gas_price,
                    pyth_price: pyth_price.clone(),
                    enriched_at: *enriched_at,
                }),
                ..entity.clone()
            })),

            Acknowledged { acknowledged_at } => Ok(Some(Self {
                acknowledged_at: Some(*acknowledged_at),
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

            #[cfg(any(test, feature = "test-support"))]
            WitnessAt {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
                filled_at,
            } => Ok(vec![Filled {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_timestamp,
                filled_at,
            }]),

            Enrich { .. } | Acknowledge => Err(OnChainTradeError::NotFilled),
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

            #[cfg(any(test, feature = "test-support"))]
            WitnessAt { .. } => Err(OnChainTradeError::AlreadyFilled),

            Enrich {
                gas_used,
                effective_gas_price,
                pyth_price,
            } => {
                if self.is_enriched() {
                    return Err(OnChainTradeError::AlreadyEnriched);
                }

                // SQLite stores integers as i64; reject values that would
                // violate the CHECK constraint on the onchain_trades table.
                if effective_gas_price > i64::MAX as u128 {
                    return Err(OnChainTradeError::GasPriceOutOfRange {
                        effective_gas_price,
                    });
                }

                Ok(vec![Enriched {
                    gas_used,
                    effective_gas_price,
                    pyth_price,
                    enriched_at: Utc::now(),
                }])
            }

            Acknowledge => {
                if self.is_acknowledged() {
                    return Err(OnChainTradeError::AlreadyAcknowledged);
                }

                Ok(vec![Acknowledged {
                    acknowledged_at: Utc::now(),
                }])
            }
        }
    }
}

impl OnChainTrade {
    pub(crate) fn is_enriched(&self) -> bool {
        self.enrichment.is_some()
    }

    /// Whether the `Position` aggregate has acknowledged this fill --
    /// the condition under which the trade-accounting dedupe treats the
    /// trade as fully processed.
    pub(crate) fn is_acknowledged(&self) -> bool {
        self.acknowledged_at.is_some()
    }

    pub(crate) fn into_trade(self, id: &OnChainTradeId) -> Trade {
        Trade {
            id: id.to_string(),
            occurred_at: self.block_timestamp,
            venue: TradingVenue::Raindex,
            direction: self.direction,
            symbol: self.symbol,
            shares: FractionalShares::new(self.amount),
            outcome: TradeOutcome::Filled,
        }
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
    #[error("Trade has already been acknowledged by the position")]
    AlreadyAcknowledged,
    #[error(
        "Effective gas price {effective_gas_price} exceeds i64::MAX \
         and cannot be stored in SQLite"
    )]
    GasPriceOutOfRange { effective_gas_price: u128 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum OnChainTradeCommand {
    Witness {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        amount: Float,
        direction: Direction,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        price_usdc: Float,
        block_number: u64,
        block_timestamp: DateTime<Utc>,
    },
    /// Test/fixture-only: identical to `Witness` but takes `filled_at`
    /// explicitly instead of stamping `Utc::now()`, so fixture seeding can
    /// backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    WitnessAt {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        amount: Float,
        direction: Direction,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        price_usdc: Float,
        block_number: u64,
        block_timestamp: DateTime<Utc>,
        filled_at: DateTime<Utc>,
    },
    Enrich {
        gas_used: u64,
        effective_gas_price: u128,
        pyth_price: PythPrice,
    },
    /// Marks the fill as acknowledged by the `Position` aggregate.
    /// Sent only after `AcknowledgeOnChainFill` succeeded, so the
    /// dedupe guard can distinguish "witnessed" from "fully accounted".
    Acknowledge,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum OnChainTradeEvent {
    Filled {
        symbol: Symbol,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        amount: Float,
        direction: Direction,
        #[serde(
            serialize_with = "st0x_float_serde::serialize_float_as_string",
            deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
        )]
        price_usdc: Float,
        block_number: u64,
        block_timestamp: DateTime<Utc>,
        filled_at: DateTime<Utc>,
    },
    Enriched {
        gas_used: u64,
        effective_gas_price: u128,
        pyth_price: PythPrice,
        enriched_at: DateTime<Utc>,
    },
    Acknowledged {
        acknowledged_at: DateTime<Utc>,
    },
}

/// Required by `cqrs_es::DomainEvent`.
impl PartialEq for OnChainTradeEvent {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Filled {
                    symbol: sym_a,
                    amount: amt_a,
                    direction: dir_a,
                    price_usdc: price_a,
                    block_number: block_num_a,
                    block_timestamp: block_ts_a,
                    filled_at: fill_a,
                },
                Self::Filled {
                    symbol: sym_b,
                    amount: amt_b,
                    direction: dir_b,
                    price_usdc: price_b,
                    block_number: block_num_b,
                    block_timestamp: block_ts_b,
                    filled_at: fill_b,
                },
            ) => {
                sym_a == sym_b
                    && amt_a.eq(*amt_b).unwrap_or(false)
                    && dir_a == dir_b
                    && price_a.eq(*price_b).unwrap_or(false)
                    && block_num_a == block_num_b
                    && block_ts_a == block_ts_b
                    && fill_a == fill_b
            }
            (
                Self::Enriched {
                    gas_used: g1,
                    effective_gas_price: egp1,
                    pyth_price: pp1,
                    enriched_at: e1,
                },
                Self::Enriched {
                    gas_used: g2,
                    effective_gas_price: egp2,
                    pyth_price: pp2,
                    enriched_at: e2,
                },
            ) => g1 == g2 && egp1 == egp2 && pp1 == pp2 && e1 == e2,
            (
                Self::Acknowledged {
                    acknowledged_at: a1,
                },
                Self::Acknowledged {
                    acknowledged_at: a2,
                },
            ) => a1 == a2,
            _ => false,
        }
    }
}

impl Eq for OnChainTradeEvent {}

impl DomainEvent for OnChainTradeEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Filled { .. } => "OnChainTradeEvent::Filled".to_string(),
            Self::Enriched { .. } => "OnChainTradeEvent::Enriched".to_string(),
            Self::Acknowledged { .. } => "OnChainTradeEvent::Acknowledged".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Enrichment {
    pub(crate) gas_used: u64,
    pub(crate) effective_gas_price: u128,
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
    use st0x_event_sorcery::{LifecycleError, TestHarness, replay};

    use super::*;
    use st0x_float_macro::float;

    #[tokio::test]
    async fn witness_command_creates_filled_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let events = TestHarness::<OnChainTrade>::with(())
            .given_no_previous_events()
            .when(OnChainTradeCommand::Witness {
                symbol: symbol.clone(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_timestamp: now,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OnChainTradeEvent::Filled { .. }));
    }

    /// Covers the fixture-only `WitnessAt` sibling of `Witness`: it must
    /// thread the caller-supplied `filled_at` through to the emitted event's
    /// field rather than silently falling back to `Utc::now()`.
    #[tokio::test]
    async fn witness_at_uses_supplied_timestamp() {
        let symbol = Symbol::new("AAPL").unwrap();
        let block_timestamp = Utc::now();
        let filled_at = block_timestamp - chrono::Duration::hours(4);

        let events = TestHarness::<OnChainTrade>::with(())
            .given_no_previous_events()
            .when(OnChainTradeCommand::WitnessAt {
                symbol: symbol.clone(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_timestamp,
                filled_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let OnChainTradeEvent::Filled {
            filled_at: event_filled_at,
            ..
        } = &events[0]
        else {
            panic!("Expected Filled, got: {:?}", events[0]);
        };
        assert_eq!(*event_filled_at, filled_at);
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
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_timestamp: now,
                filled_at: now,
            }])
            .when(OnChainTradeCommand::Enrich {
                gas_used: 50000,
                effective_gas_price: 1_000_000_000,
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
                    amount: float!(10.5),
                    direction: Direction::Buy,
                    price_usdc: float!(150.25),
                    block_number: 12345,
                    block_timestamp: now,
                    filled_at: now,
                },
                OnChainTradeEvent::Enriched {
                    gas_used: 50000,
                    effective_gas_price: 1_000_000_000,
                    pyth_price: pyth_price.clone(),
                    enriched_at: now,
                },
            ])
            .when(OnChainTradeCommand::Enrich {
                gas_used: 50000,
                effective_gas_price: 1_000_000_000,
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
                effective_gas_price: 1_000_000_000,
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
    async fn rejects_gas_price_exceeding_i64_max() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let error = TestHarness::<OnChainTrade>::with(())
            .given(vec![OnChainTradeEvent::Filled {
                symbol,
                amount: float!("10.5"),
                direction: Direction::Buy,
                price_usdc: float!("150.25"),
                block_number: 12345,
                block_timestamp: now,
                filled_at: now,
            }])
            .when(OnChainTradeCommand::Enrich {
                gas_used: 50000,
                effective_gas_price: (i64::MAX as u128) + 1,
                pyth_price,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::GasPriceOutOfRange { .. })
        ));
    }

    #[tokio::test]
    async fn acknowledge_marks_witnessed_trade() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let events = TestHarness::<OnChainTrade>::with(())
            .given(vec![OnChainTradeEvent::Filled {
                symbol,
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_timestamp: now,
                filled_at: now,
            }])
            .when(OnChainTradeCommand::Acknowledge)
            .await
            .events();

        assert!(
            matches!(events.as_slice(), [OnChainTradeEvent::Acknowledged { .. }]),
            "Acknowledge on a witnessed trade must emit the marker; got {events:?}",
        );
    }

    #[tokio::test]
    async fn cannot_acknowledge_twice() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let error = TestHarness::<OnChainTrade>::with(())
            .given(vec![
                OnChainTradeEvent::Filled {
                    symbol,
                    amount: float!(10.5),
                    direction: Direction::Buy,
                    price_usdc: float!(150.25),
                    block_number: 12345,
                    block_timestamp: now,
                    filled_at: now,
                },
                OnChainTradeEvent::Acknowledged {
                    acknowledged_at: now,
                },
            ])
            .when(OnChainTradeCommand::Acknowledge)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::AlreadyAcknowledged)
        ));
    }

    #[tokio::test]
    async fn cannot_acknowledge_unwitnessed_trade() {
        let error = TestHarness::<OnChainTrade>::with(())
            .given_no_previous_events()
            .when(OnChainTradeCommand::Acknowledge)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::NotFilled)
        ));
    }

    /// Production emits `Enriched` before `Acknowledged`: enrichment runs
    /// in the fresh-trade path, then the position acknowledges the fill.
    /// The `Acknowledged` evolve handler must preserve the enrichment it
    /// finds so both markers survive in the live aggregate.
    #[tokio::test]
    async fn acknowledge_after_enrich_preserves_both_markers() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let trade = replay::<OnChainTrade>(vec![
            OnChainTradeEvent::Filled {
                symbol,
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_timestamp: now,
                filled_at: now,
            },
            OnChainTradeEvent::Enriched {
                gas_used: 21000,
                effective_gas_price: 100,
                pyth_price: PythPrice {
                    value: "150250000".to_string(),
                    expo: -6,
                    conf: "50000".to_string(),
                    publish_time: now,
                },
                enriched_at: now,
            },
            OnChainTradeEvent::Acknowledged {
                acknowledged_at: now,
            },
        ])
        .unwrap()
        .expect("replay must produce a live trade");

        assert!(trade.is_acknowledged());
        assert!(trade.is_enriched());
    }

    #[tokio::test]
    async fn cannot_witness_twice_when_filled() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let error = TestHarness::<OnChainTrade>::with(())
            .given(vec![OnChainTradeEvent::Filled {
                symbol: symbol.clone(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_timestamp: now,
                filled_at: now,
            }])
            .when(OnChainTradeCommand::Witness {
                symbol: symbol.clone(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
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
                    amount: float!(10.5),
                    direction: Direction::Buy,
                    price_usdc: float!(150.25),
                    block_number: 12345,
                    block_timestamp: now,
                    filled_at: now,
                },
                OnChainTradeEvent::Enriched {
                    gas_used: 50000,
                    effective_gas_price: 1_000_000_000,
                    pyth_price,
                    enriched_at: now,
                },
            ])
            .when(OnChainTradeCommand::Witness {
                symbol: symbol.clone(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
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
            amount: float!(10.5),
            direction: Direction::Buy,
            price_usdc: float!(150.25),
            block_number: 12345,
            block_timestamp: now,
            filled_at: now,
        }])
        .unwrap()
        .unwrap();

        assert_eq!(trade.symbol, Symbol::new("AAPL").unwrap());
        assert!(trade.amount.eq(float!(10.5)).unwrap());
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
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_timestamp: now,
                filled_at: now,
            },
            OnChainTradeEvent::Enriched {
                gas_used: 50000,
                effective_gas_price: 1_000_000_000,
                pyth_price: pyth_price.clone(),
                enriched_at: now,
            },
        ])
        .unwrap()
        .unwrap();

        assert!(trade.is_enriched());
        let enrichment = trade.enrichment.unwrap();
        assert_eq!(enrichment.gas_used, 50000);
        assert_eq!(enrichment.effective_gas_price, 1_000_000_000);
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
            effective_gas_price: 1_000_000_000,
            pyth_price,
            enriched_at: Utc::now(),
        }])
        .unwrap_err();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
    }
}
