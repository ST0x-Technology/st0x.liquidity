//! OnChainTrade CQRS/ES aggregate for recording direct Raindex orderbook fills
//! and fills routed through shared-inventory adapters.
//!
//! Keyed by `(chain, tx_hash, log_index)`: a transaction hash is unique only
//! within one chain, so the chain is part of the identity, rendered as a
//! uniform `chain:tx_hash:log_index` aggregate id (RAI-2078 migrated legacy
//! bare ids in place). Historical enrichment events remain replayable, but
//! current trade accounting no longer emits them (ADR 0020).

use std::num::ParseIntError;
use std::str::FromStr;

use alloy::hex::FromHexError;
use alloy::primitives::{Address, B256, TxHash};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use st0x_dto::{Direction, Trade, TradeOutcome, TradingVenue};
use st0x_event_sorcery::{DomainEvent, EventSourced, Table};
use st0x_evm::{Chain, ParseChainError};
use st0x_execution::Symbol;
use st0x_finance::{FractionalShares, NotPositive, Positive};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OnChainTradeId {
    pub(crate) chain: Chain,
    pub(crate) tx_hash: TxHash,
    pub(crate) log_index: u64,
}

impl OnChainTradeId {
    pub fn new(chain: Chain, tx_hash: TxHash, log_index: u64) -> Self {
        Self {
            chain,
            tx_hash,
            log_index,
        }
    }
}

impl std::fmt::Display for OnChainTradeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.chain, self.tx_hash, self.log_index)
    }
}

#[derive(Debug, Error)]
pub enum ParseOnChainTradeIdError {
    #[error("expected 'chain:tx_hash:log_index', got '{id_provided}'")]
    MissingDelimiter { id_provided: String },
    #[error("invalid chain: {0}")]
    Chain(#[from] ParseChainError),
    #[error("invalid tx_hash: {0}")]
    TxHash(#[from] FromHexError),
    #[error("invalid log_index: {0}")]
    LogIndex(#[from] ParseIntError),
}

impl FromStr for OnChainTradeId {
    type Err = ParseOnChainTradeIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (chain_str, rest) =
            value
                .split_once(':')
                .ok_or_else(|| ParseOnChainTradeIdError::MissingDelimiter {
                    id_provided: value.to_string(),
                })?;
        let chain = chain_str.parse()?;
        let (tx_hash_str, log_index_str) =
            rest.split_once(':')
                .ok_or_else(|| ParseOnChainTradeIdError::MissingDelimiter {
                    id_provided: value.to_string(),
                })?;
        let tx_hash = tx_hash_str.parse()?;
        let log_index = log_index_str.parse()?;
        Ok(Self {
            chain,
            tx_hash,
            log_index,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnChainTrade {
    #[serde(default = "legacy_source")]
    pub(crate) source: OnChainTradeSource,
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
    /// Hash of the block this fill was confirmed in, persisted so a later
    /// replay against a different fork is detectable as a reorg rather than a
    /// duplicate. Absent on aggregates persisted before the field existed and on
    /// fills whose source log carried no block hash.
    #[serde(default)]
    pub(crate) block_hash: Option<B256>,
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
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "OnChainTrade";
    const PROJECTION: Table = Table("onchain_trade_view");
    const SCHEMA_VERSION: u64 = 4;

    fn originate(event: &Self::Event) -> Option<Self> {
        use OnChainTradeEvent::*;
        match event {
            Filled {
                source,
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_hash,
                block_timestamp,
                filled_at,
            } => Some(Self {
                source: *source,
                symbol: symbol.clone(),
                amount: *amount,
                direction: *direction,
                price_usdc: *price_usdc,
                block_number: Some(*block_number),
                block_hash: *block_hash,
                block_timestamp: *block_timestamp,
                filled_at: *filled_at,
                enrichment: None,
                acknowledged_at: None,
            }),

            SourceAttributed { .. } | Enriched { .. } | Acknowledged { .. } => None,
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

            SourceAttributed { source, .. } => Ok(Some(Self {
                source: *source,
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
                source,
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_hash,
                block_timestamp,
            } => {
                match source {
                    OnChainTradeSource::Legacy => {
                        return Err(OnChainTradeError::LegacySourceWitness);
                    }
                    OnChainTradeSource::Raindex
                    | OnChainTradeSource::Inventory { .. }
                    | OnChainTradeSource::UnrecognizedInventory { .. } => {}
                }

                log_unrecognized_inventory_source(source);

                Ok(vec![Filled {
                    source,
                    symbol,
                    amount,
                    direction,
                    price_usdc,
                    block_number,
                    block_hash,
                    block_timestamp,
                    filled_at: Utc::now(),
                }])
            }

            #[cfg(any(test, feature = "test-support"))]
            WitnessAt {
                source,
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_hash,
                block_timestamp,
                filled_at,
            } => Ok(vec![Filled {
                source,
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_hash,
                block_timestamp,
                filled_at,
            }]),

            AttributeSource { .. } | Acknowledge => Err(OnChainTradeError::NotFilled),
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

            AttributeSource { source } => {
                match self.source.attribution_decision(source) {
                    SourceAttributionDecision::Apply => {}
                    SourceAttributionDecision::AlreadyAttributed => {
                        return Err(OnChainTradeError::SourceAlreadyAttributed);
                    }
                    SourceAttributionDecision::InvalidLegacyMarker => {
                        return Err(OnChainTradeError::LegacySourceAttribution);
                    }
                }

                log_unrecognized_inventory_source(source);

                Ok(vec![SourceAttributed {
                    source,
                    attributed_at: Utc::now(),
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
    /// Whether the `Position` aggregate has acknowledged this fill --
    /// the condition under which the trade-accounting dedupe treats the
    /// trade as fully processed.
    pub fn is_acknowledged(&self) -> bool {
        self.acknowledged_at.is_some()
    }

    pub fn source(&self) -> OnChainTradeSource {
        self.source
    }

    pub(crate) fn try_into_trade(
        self,
        id: &OnChainTradeId,
    ) -> Result<Trade, NotPositive<FractionalShares>> {
        Ok(Trade {
            id: id.to_string(),
            occurred_at: self.block_timestamp,
            venue: self.source.trading_venue(),
            direction: self.direction,
            symbol: self.symbol,
            shares: Positive::new(FractionalShares::new(self.amount))?,
            outcome: TradeOutcome::Filled,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum InventoryVenue {
    Bebop,
    UniswapV4,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OnChainTradeSource {
    /// Filled event persisted before source attribution existed. Displays as
    /// Raindex until a chain-backed repair appends `SourceAttributed`.
    Legacy,
    Raindex,
    Inventory {
        operator: Address,
        venue: InventoryVenue,
    },
    UnrecognizedInventory {
        operator: Address,
    },
}

const fn legacy_source() -> OnChainTradeSource {
    OnChainTradeSource::Legacy
}

impl OnChainTradeSource {
    pub(crate) fn trading_venue(self) -> TradingVenue {
        match self {
            Self::Legacy | Self::Raindex => TradingVenue::Raindex,
            Self::Inventory {
                venue: InventoryVenue::Bebop,
                ..
            } => TradingVenue::Bebop,
            Self::Inventory {
                venue: InventoryVenue::UniswapV4,
                ..
            } => TradingVenue::UniswapV4,
            Self::UnrecognizedInventory { .. } => TradingVenue::UnknownOnchain,
        }
    }

    fn unrecognized_inventory_operator(self) -> Option<Address> {
        match self {
            Self::Legacy | Self::Raindex | Self::Inventory { .. } => None,
            Self::UnrecognizedInventory { operator } => Some(operator),
        }
    }

    pub(crate) fn attribution_decision(self, replacement: Self) -> SourceAttributionDecision {
        match self {
            Self::Legacy => match replacement {
                Self::Legacy => SourceAttributionDecision::InvalidLegacyMarker,
                Self::Raindex | Self::Inventory { .. } | Self::UnrecognizedInventory { .. } => {
                    SourceAttributionDecision::Apply
                }
            },
            Self::UnrecognizedInventory { operator } => match replacement {
                Self::Inventory {
                    operator: attributed_operator,
                    ..
                } if attributed_operator == operator => SourceAttributionDecision::Apply,
                Self::Legacy
                | Self::Raindex
                | Self::Inventory { .. }
                | Self::UnrecognizedInventory { .. } => {
                    SourceAttributionDecision::AlreadyAttributed
                }
            },
            Self::Raindex | Self::Inventory { .. } => SourceAttributionDecision::AlreadyAttributed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceAttributionDecision {
    Apply,
    AlreadyAttributed,
    InvalidLegacyMarker,
}

fn log_unrecognized_inventory_source(source: OnChainTradeSource) {
    if let Some(operator) = source.unrecognized_inventory_operator() {
        warn!(
            %operator,
            "Inventory fill uses an unrecognized operator; attributing it to Unknown Onchain"
        );
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub enum OnChainTradeError {
    #[error("Cannot update trade that hasn't been filled yet")]
    NotFilled,
    #[error("Trade has already been filled")]
    AlreadyFilled,
    #[error("Trade source has already been attributed")]
    SourceAlreadyAttributed,
    #[error("A legacy source marker cannot be used as a source attribution")]
    LegacySourceAttribution,
    #[error("A legacy source marker cannot be used when witnessing a live trade")]
    LegacySourceWitness,
    #[error("Trade has already been acknowledged by the position")]
    AlreadyAcknowledged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OnChainTradeCommand {
    Witness {
        source: OnChainTradeSource,
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
        block_hash: Option<B256>,
        block_timestamp: DateTime<Utc>,
    },
    /// Repairs a source-less legacy fill after re-reading its operator from
    /// chain, or upgrades an unrecognized inventory source after its same
    /// operator is configured.
    AttributeSource { source: OnChainTradeSource },
    /// Test/fixture-only: identical to `Witness` but takes `filled_at`
    /// explicitly instead of stamping `Utc::now()`, so fixture seeding can
    /// backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    WitnessAt {
        source: OnChainTradeSource,
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
        block_hash: Option<B256>,
        block_timestamp: DateTime<Utc>,
        filled_at: DateTime<Utc>,
    },
    /// Marks the fill as acknowledged by the `Position` aggregate.
    /// Sent only after `AcknowledgeOnChainFill` succeeded, so the
    /// dedupe guard can distinguish "witnessed" from "fully accounted".
    Acknowledge,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OnChainTradeEvent {
    Filled {
        #[serde(default = "legacy_source")]
        source: OnChainTradeSource,
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
        #[serde(default)]
        block_hash: Option<B256>,
        block_timestamp: DateTime<Utc>,
        filled_at: DateTime<Utc>,
    },
    SourceAttributed {
        source: OnChainTradeSource,
        attributed_at: DateTime<Utc>,
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
                    source: source_a,
                    symbol: sym_a,
                    amount: amt_a,
                    direction: dir_a,
                    price_usdc: price_a,
                    block_number: block_num_a,
                    block_hash: block_hash_a,
                    block_timestamp: block_ts_a,
                    filled_at: fill_a,
                },
                Self::Filled {
                    source: source_b,
                    symbol: sym_b,
                    amount: amt_b,
                    direction: dir_b,
                    price_usdc: price_b,
                    block_number: block_num_b,
                    block_hash: block_hash_b,
                    block_timestamp: block_ts_b,
                    filled_at: fill_b,
                },
            ) => {
                source_a == source_b
                    && sym_a == sym_b
                    && amt_a.eq(*amt_b).unwrap_or(false)
                    && dir_a == dir_b
                    && price_a.eq(*price_b).unwrap_or(false)
                    && block_num_a == block_num_b
                    && block_hash_a == block_hash_b
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
            (
                Self::SourceAttributed {
                    source: source_a,
                    attributed_at: attributed_a,
                },
                Self::SourceAttributed {
                    source: source_b,
                    attributed_at: attributed_b,
                },
            ) => source_a == source_b && attributed_a == attributed_b,
            _ => false,
        }
    }
}

impl Eq for OnChainTradeEvent {}

impl DomainEvent for OnChainTradeEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Filled { .. } => "OnChainTradeEvent::Filled".to_string(),
            Self::SourceAttributed { .. } => "OnChainTradeEvent::SourceAttributed".to_string(),
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
pub struct PythPrice {
    pub(crate) value: String,
    pub(crate) expo: i32,
    pub(crate) conf: String,
    pub(crate) publish_time: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use alloy::primitives::b256;
    use st0x_event_sorcery::{LifecycleError, TestHarness, replay};
    use st0x_float_macro::float;

    use super::*;

    #[tokio::test]
    async fn witness_command_creates_filled_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let events = TestHarness::<OnChainTrade>::with(())
            .given_no_previous_events()
            .when(OnChainTradeCommand::Witness {
                source: OnChainTradeSource::Raindex,
                symbol: symbol.clone(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
                block_timestamp: now,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], OnChainTradeEvent::Filled { .. }));
    }

    #[tokio::test]
    async fn witness_command_rejects_legacy_source_markers() {
        let error = TestHarness::<OnChainTrade>::with(())
            .given_no_previous_events()
            .when(OnChainTradeCommand::Witness {
                source: OnChainTradeSource::Legacy,
                symbol: Symbol::new("AAPL").unwrap(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
                block_timestamp: Utc::now(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::LegacySourceWitness)
        ));
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
                source: OnChainTradeSource::Raindex,
                symbol: symbol.clone(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
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
    async fn witness_threads_block_hash_into_filled_event() {
        let now = Utc::now();
        let block_hash =
            b256!("0xabababababababababababababababababababababababababababababababab");

        let events = TestHarness::<OnChainTrade>::with(())
            .given_no_previous_events()
            .when(OnChainTradeCommand::Witness {
                source: OnChainTradeSource::Raindex,
                symbol: Symbol::new("AAPL").unwrap(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: Some(block_hash),
                block_timestamp: now,
            })
            .await
            .events();

        let [
            OnChainTradeEvent::Filled {
                block_hash: emitted,
                ..
            },
        ] = events.as_slice()
        else {
            panic!("expected a single Filled event, got {events:?}");
        };
        assert_eq!(*emitted, Some(block_hash));
    }

    #[tokio::test]
    async fn acknowledge_marks_witnessed_trade() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let events = TestHarness::<OnChainTrade>::with(())
            .given(vec![OnChainTradeEvent::Filled {
                source: OnChainTradeSource::Raindex,
                symbol,
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
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
                    source: OnChainTradeSource::Raindex,
                    symbol,
                    amount: float!(10.5),
                    direction: Direction::Buy,
                    price_usdc: float!(150.25),
                    block_number: 12345,
                    block_hash: None,
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

    /// Persisted trades may contain a legacy `Enriched` event. Replaying a
    /// later acknowledgement must preserve that historical data.
    #[tokio::test]
    async fn acknowledge_after_enrich_preserves_both_markers() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let trade = replay::<OnChainTrade>(vec![
            OnChainTradeEvent::Filled {
                source: OnChainTradeSource::Raindex,
                symbol,
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
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
        assert!(trade.enrichment.is_some());
    }

    #[tokio::test]
    async fn cannot_witness_twice_when_filled() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let error = TestHarness::<OnChainTrade>::with(())
            .given(vec![OnChainTradeEvent::Filled {
                source: OnChainTradeSource::Raindex,
                symbol: symbol.clone(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
                block_timestamp: now,
                filled_at: now,
            }])
            .when(OnChainTradeCommand::Witness {
                source: OnChainTradeSource::Raindex,
                symbol: symbol.clone(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
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
                    source: OnChainTradeSource::Raindex,
                    symbol: symbol.clone(),
                    amount: float!(10.5),
                    direction: Direction::Buy,
                    price_usdc: float!(150.25),
                    block_number: 12345,
                    block_hash: None,
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
                source: OnChainTradeSource::Raindex,
                symbol: symbol.clone(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
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
            source: OnChainTradeSource::Raindex,
            symbol,
            amount: float!(10.5),
            direction: Direction::Buy,
            price_usdc: float!(150.25),
            block_number: 12345,
            block_hash: None,
            block_timestamp: now,
            filled_at: now,
        }])
        .unwrap()
        .unwrap();

        assert_eq!(trade.symbol, Symbol::new("AAPL").unwrap());
        assert!(trade.amount.eq(float!(10.5)).unwrap());
        assert_eq!(trade.direction, Direction::Buy);
        assert!(trade.enrichment.is_none());
    }

    #[test]
    fn filled_carries_block_hash_into_live_state() {
        let block_hash =
            b256!("0xabababababababababababababababababababababababababababababababab");
        let now = Utc::now();

        let trade = replay::<OnChainTrade>(vec![OnChainTradeEvent::Filled {
            source: OnChainTradeSource::Raindex,
            symbol: Symbol::new("AAPL").unwrap(),
            amount: float!(10.5),
            direction: Direction::Buy,
            price_usdc: float!(150.25),
            block_number: 12345,
            block_hash: Some(block_hash),
            block_timestamp: now,
            filled_at: now,
        }])
        .unwrap()
        .unwrap();

        assert_eq!(trade.block_hash, Some(block_hash));
    }

    /// Fills persisted before `block_hash` existed deserialize with `None`
    /// rather than failing replay (the `#[serde(default)]` resume guarantee).
    #[test]
    fn filled_event_without_block_hash_deserializes_to_none() {
        let now = Utc::now();
        let legacy_filled = serde_json::json!({
            "Filled": {
                "symbol": "AAPL",
                "amount": "10.5",
                "direction": "Buy",
                "price_usdc": "150.25",
                "block_number": 12345,
                "block_timestamp": now,
                "filled_at": now,
            }
        });

        let event: OnChainTradeEvent = serde_json::from_value(legacy_filled).unwrap();
        let OnChainTradeEvent::Filled { block_hash, .. } = event else {
            panic!("expected Filled, got {event:?}");
        };
        assert_eq!(block_hash, None);
    }

    #[test]
    fn legacy_filled_event_without_source_retains_repairable_marker() {
        let now = Utc::now();
        let event = OnChainTradeEvent::Filled {
            source: OnChainTradeSource::Inventory {
                operator: Address::repeat_byte(0x8b),
                venue: InventoryVenue::Bebop,
            },
            symbol: Symbol::new("AAPL").unwrap(),
            amount: float!(10.5),
            direction: Direction::Buy,
            price_usdc: float!(150.25),
            block_number: 12345,
            block_hash: None,
            block_timestamp: now,
            filled_at: now,
        };
        let mut payload = serde_json::to_value(event).unwrap();
        payload["Filled"].as_object_mut().unwrap().remove("source");

        let legacy: OnChainTradeEvent = serde_json::from_value(payload).unwrap();
        let OnChainTradeEvent::Filled { source, .. } = legacy else {
            panic!("expected Filled event");
        };

        assert_eq!(source, OnChainTradeSource::Legacy);
        assert_eq!(source.trading_venue(), TradingVenue::Raindex);
    }

    #[test]
    fn inventory_source_maps_persisted_adapter_venue() {
        assert_eq!(
            OnChainTradeSource::Inventory {
                operator: Address::repeat_byte(0x01),
                venue: InventoryVenue::Bebop,
            }
            .trading_venue(),
            TradingVenue::Bebop
        );
        assert_eq!(
            OnChainTradeSource::Inventory {
                operator: Address::repeat_byte(0x01),
                venue: InventoryVenue::UniswapV4,
            }
            .trading_venue(),
            TradingVenue::UniswapV4
        );
        assert_eq!(
            OnChainTradeSource::UnrecognizedInventory {
                operator: Address::repeat_byte(0x01),
            }
            .trading_venue(),
            TradingVenue::UnknownOnchain
        );
    }

    #[tokio::test]
    async fn source_attribution_repairs_a_legacy_fill() {
        let now = Utc::now();
        let source = OnChainTradeSource::Inventory {
            operator: Address::repeat_byte(0x8b),
            venue: InventoryVenue::Bebop,
        };
        let filled = OnChainTradeEvent::Filled {
            source: OnChainTradeSource::Legacy,
            symbol: Symbol::new("AAPL").unwrap(),
            amount: float!(1),
            direction: Direction::Buy,
            price_usdc: float!(150),
            block_number: 12345,
            block_hash: None,
            block_timestamp: now,
            filled_at: now,
        };

        let events = TestHarness::<OnChainTrade>::with(())
            .given(vec![filled.clone()])
            .when(OnChainTradeCommand::AttributeSource { source })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [OnChainTradeEvent::SourceAttributed {
                source: attributed,
                ..
            }] if *attributed == source
        ));
        let repaired = replay::<OnChainTrade>(vec![filled, events[0].clone()])
            .unwrap()
            .unwrap();
        assert_eq!(repaired.source, source);
        assert_eq!(
            repaired
                .try_into_trade(&OnChainTradeId {
                    chain: Chain::Base,
                    tx_hash: TxHash::ZERO,
                    log_index: 0,
                })
                .unwrap()
                .venue,
            TradingVenue::Bebop
        );
    }

    #[tokio::test]
    async fn source_attribution_upgrades_a_matching_unrecognized_operator() {
        let now = Utc::now();
        let operator = Address::repeat_byte(0x8b);
        let source = OnChainTradeSource::Inventory {
            operator,
            venue: InventoryVenue::Bebop,
        };
        let filled = OnChainTradeEvent::Filled {
            source: OnChainTradeSource::UnrecognizedInventory { operator },
            symbol: Symbol::new("AAPL").unwrap(),
            amount: float!(1),
            direction: Direction::Buy,
            price_usdc: float!(150),
            block_number: 12345,
            block_hash: None,
            block_timestamp: now,
            filled_at: now,
        };

        let events = TestHarness::<OnChainTrade>::with(())
            .given(vec![filled.clone()])
            .when(OnChainTradeCommand::AttributeSource { source })
            .await
            .events();

        assert!(matches!(
            events.as_slice(),
            [OnChainTradeEvent::SourceAttributed {
                source: attributed,
                ..
            }] if *attributed == source
        ));
        let repaired = replay::<OnChainTrade>(vec![filled, events[0].clone()])
            .unwrap()
            .unwrap();
        assert_eq!(repaired.source, source);
        assert_eq!(
            repaired
                .try_into_trade(&OnChainTradeId {
                    chain: Chain::Base,
                    tx_hash: TxHash::ZERO,
                    log_index: 0,
                })
                .unwrap()
                .venue,
            TradingVenue::Bebop
        );
    }

    #[tokio::test]
    async fn source_attribution_rejects_a_different_inventory_operator() {
        let now = Utc::now();
        let error = TestHarness::<OnChainTrade>::with(())
            .given(vec![OnChainTradeEvent::Filled {
                source: OnChainTradeSource::UnrecognizedInventory {
                    operator: Address::repeat_byte(0x01),
                },
                symbol: Symbol::new("AAPL").unwrap(),
                amount: float!(1),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_number: 12345,
                block_hash: None,
                block_timestamp: now,
                filled_at: now,
            }])
            .when(OnChainTradeCommand::AttributeSource {
                source: OnChainTradeSource::Inventory {
                    operator: Address::repeat_byte(0x02),
                    venue: InventoryVenue::Bebop,
                },
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::SourceAlreadyAttributed)
        ));
    }

    #[tokio::test]
    async fn source_attribution_rejects_an_already_attributed_fill() {
        let now = Utc::now();
        let error = TestHarness::<OnChainTrade>::with(())
            .given(vec![OnChainTradeEvent::Filled {
                source: OnChainTradeSource::Raindex,
                symbol: Symbol::new("AAPL").unwrap(),
                amount: float!(1),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_number: 12345,
                block_hash: None,
                block_timestamp: now,
                filled_at: now,
            }])
            .when(OnChainTradeCommand::AttributeSource {
                source: OnChainTradeSource::UnrecognizedInventory {
                    operator: Address::repeat_byte(0x01),
                },
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::SourceAlreadyAttributed)
        ));
    }

    #[tokio::test]
    async fn source_attribution_rejects_legacy_source_markers() {
        let now = Utc::now();
        let error = TestHarness::<OnChainTrade>::with(())
            .given(vec![OnChainTradeEvent::Filled {
                source: OnChainTradeSource::Legacy,
                symbol: Symbol::new("AAPL").unwrap(),
                amount: float!(1),
                direction: Direction::Buy,
                price_usdc: float!(150),
                block_number: 12345,
                block_hash: None,
                block_timestamp: now,
                filled_at: now,
            }])
            .when(OnChainTradeCommand::AttributeSource {
                source: OnChainTradeSource::Legacy,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::LegacySourceAttribution)
        ));
    }

    /// Runs the identity migration's UPDATE section over raw legacy-shaped
    /// rows and asserts they come out parseable under current code. The
    /// direct INSERTs bypass the framework deliberately: they reproduce the
    /// pre-migration on-disk shape, which no current code path can produce.
    #[tokio::test]
    async fn identity_migration_upgrades_legacy_rows() {
        let pool = crate::test_utils::setup_test_db().await;

        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, \
             event_version, payload, metadata) VALUES \
             ('OnChainTrade', \
              '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:7', \
              1, 'OnChainTradeEvent::Filled', '1.0', '{}', '{}'), \
             ('Position', 'AAPL', 1, 'PositionEvent::OnChainOrderFilled', '1.0', \
              '{\"OnChainOrderFilled\":{\"trade_id\":{\"tx_hash\":\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"log_index\":7}}}', '{}')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO dashboard_trade_delivery (trade_id, delivered_at) VALUES \
             ('0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:7', \
              '2026-01-01T00:00:00+00:00')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let migration =
            include_str!("../migrations/20260901124004_chain_qualified_fill_identity.sql");
        let (updates, _table_rebuilds) = migration
            .split_once("-- hedge_fill:")
            .expect("the migration must keep its UPDATE section above the table rebuilds");
        // Twice: the WHERE guards make the upgrades idempotent.
        sqlx::raw_sql(updates).execute(&pool).await.unwrap();
        sqlx::raw_sql(updates).execute(&pool).await.unwrap();

        let (aggregate_id,): (String,) =
            sqlx::query_as("SELECT aggregate_id FROM events WHERE aggregate_type = 'OnChainTrade'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let parsed: OnChainTradeId = aggregate_id.parse().unwrap();
        assert_eq!(parsed.chain, Chain::Base);
        assert_eq!(parsed.log_index, 7);

        let (chain_in_payload,): (String,) = sqlx::query_as(
            "SELECT json_extract(payload, '$.OnChainOrderFilled.trade_id.chain') \
             FROM events WHERE aggregate_type = 'Position'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(chain_in_payload, "base");

        let (delivery_id,): (String,) =
            sqlx::query_as("SELECT trade_id FROM dashboard_trade_delivery")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(delivery_id.starts_with("base:0x"), "got {delivery_id}");
    }

    #[test]
    fn trade_id_renders_chain_qualified_on_every_chain() {
        // The aggregate id is persisted; each spelling is pinned as a literal
        // so a rendering change cannot slip through as a refactor.
        let tx_hash: TxHash = "0x1111111111111111111111111111111111111111111111111111111111111111"
            .parse()
            .unwrap();

        for (chain, expected) in [
            (
                Chain::Base,
                "base:0x1111111111111111111111111111111111111111111111111111111111111111:7",
            ),
            (
                Chain::Ethereum,
                "ethereum:0x1111111111111111111111111111111111111111111111111111111111111111:7",
            ),
            (
                Chain::HyperEvm,
                "hyperevm:0x1111111111111111111111111111111111111111111111111111111111111111:7",
            ),
        ] {
            let id = OnChainTradeId {
                chain,
                tx_hash,
                log_index: 7,
            };

            assert_eq!(id.to_string(), expected);
            assert_eq!(expected.parse::<OnChainTradeId>().unwrap(), id);
        }
    }

    #[test]
    fn trade_id_rejects_the_retired_bare_form() {
        // The identity migration prefixed every persisted id, so a bare
        // two-part id can only be a bug; it must not silently read as Base.
        let error = "0x1111111111111111111111111111111111111111111111111111111111111111:7"
            .parse::<OnChainTradeId>()
            .unwrap_err();

        assert!(
            matches!(error, ParseOnChainTradeIdError::Chain(_)),
            "got {error:?}"
        );
    }

    #[test]
    fn trade_id_rejects_an_unknown_chain_prefix() {
        let error = "solana:0x1111111111111111111111111111111111111111111111111111111111111111:7"
            .parse::<OnChainTradeId>()
            .unwrap_err();

        assert!(
            matches!(error, ParseOnChainTradeIdError::Chain(_)),
            "got {error:?}"
        );
    }

    #[test]
    fn dashboard_trade_rejects_non_positive_fill_quantity() {
        let now = Utc::now();
        let id = OnChainTradeId {
            chain: Chain::Base,
            tx_hash: TxHash::ZERO,
            log_index: 0,
        };

        for amount in [float!(0), float!(-1)] {
            let trade = replay::<OnChainTrade>(vec![OnChainTradeEvent::Filled {
                source: OnChainTradeSource::Raindex,
                symbol: Symbol::new("AAPL").unwrap(),
                amount,
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
                block_timestamp: now,
                filled_at: now,
            }])
            .unwrap()
            .unwrap();

            trade.try_into_trade(&id).unwrap_err();
        }
    }

    #[test]
    fn legacy_enriched_event_updates_live_state() {
        let now = Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let trade = replay::<OnChainTrade>(vec![
            OnChainTradeEvent::Filled {
                source: OnChainTradeSource::Raindex,
                symbol: Symbol::new("AAPL").unwrap(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
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

        assert!(trade.enrichment.is_some());
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
