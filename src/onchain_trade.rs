//! OnChainTrade CQRS/ES aggregate for recording DEX fills
//! from the Raindex orderbook.
//!
//! Keyed by `(tx_hash, log_index)`. Can be enriched after
//! the fact with gas costs and Pyth oracle price data.

use std::num::ParseIntError;
use std::str::FromStr;

use alloy::hex::FromHexError;
use alloy::primitives::{B256, TxHash};
use chrono::{DateTime, Utc};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use st0x_dto::{Direction, Trade, TradingVenue};
use st0x_event_sorcery::{DomainEvent, EventSourced, JobQueue, Nil};
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
    /// Set once a reorg has invalidated this fill's block. Append-only: the
    /// original fill fields are preserved and the trade is flagged reorged rather
    /// than deleted, so the reversal stays auditable in the event log.
    /// `OnChainTrade` has no projection (`PROJECTION: Nil`), so reorg state is
    /// read from the event log and the `Position` aggregate, not from a view.
    /// Absent on aggregates persisted before the field existed, which is the
    /// not-reorged default.
    #[serde(default)]
    pub(crate) reorged_at: Option<DateTime<Utc>>,
    /// Set once the `Position` reversal for this reorg has succeeded. While
    /// `reorged_at` means "reversal started", this marks "reversal finished on
    /// both aggregates", so a reorg job re-delivered after a crash between the
    /// two writes resumes the `Position` reversal instead of skipping it
    /// permanently (ADR 0012). Absent on aggregates persisted before the field
    /// existed, the resume-safe default.
    #[serde(default)]
    pub(crate) reorg_acknowledged_at: Option<DateTime<Utc>>,
}

impl EventSourced for OnChainTrade {
    type Id = OnChainTradeId;
    type Event = OnChainTradeEvent;
    type Command = OnChainTradeCommand;
    type Error = OnChainTradeError;
    type Jobs = Nil;
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "OnChainTrade";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 4;

    fn originate(event: &Self::Event) -> Option<Self> {
        use OnChainTradeEvent::*;
        match event {
            Filled {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_hash,
                block_timestamp,
                filled_at,
            } => Some(Self {
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
                reorged_at: None,
                reorg_acknowledged_at: None,
            }),

            Enriched { .. }
            | Acknowledged { .. }
            | Reorged { .. }
            | ReorgAcknowledged { .. }
            | ReWitnessed { .. } => None,
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

            // `reorg_depth` is intentionally not stored on aggregate state: it is
            // audit detail that survives in the event log only. State just needs
            // the `reorged_at` marker to know the fill was reversed.
            Reorged { reorged_at, .. } => Ok(Some(Self {
                reorged_at: Some(*reorged_at),
                ..entity.clone()
            })),

            ReorgAcknowledged {
                reorg_acknowledged_at,
            } => Ok(Some(Self {
                reorg_acknowledged_at: Some(*reorg_acknowledged_at),
                ..entity.clone()
            })),

            // Re-mined on a new canonical block: rewrite the block fields and
            // clear all three lifecycle markers so the fill returns to a fresh
            // post-witness state, re-acknowledgeable through the normal path. The
            // fill content (symbol/amount/direction/price_usdc) and `filled_at`
            // are preserved via `..entity.clone()`; only the block changed.
            ReWitnessed {
                block_number,
                block_hash,
                block_timestamp,
                ..
            } => Ok(Some(Self {
                block_number: Some(*block_number),
                block_hash: *block_hash,
                block_timestamp: *block_timestamp,
                acknowledged_at: None,
                reorged_at: None,
                reorg_acknowledged_at: None,
                ..entity.clone()
            })),

            Filled { .. } => Ok(None),
        }
    }

    fn initialize(
        command: Self::Command,
        _jobs: &mut JobQueue<Self::Jobs>,
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
                block_hash,
                block_timestamp,
            } => Ok(vec![Filled {
                symbol,
                amount,
                direction,
                price_usdc,
                block_number,
                block_hash,
                block_timestamp,
                filled_at: Utc::now(),
            }]),

            Enrich { .. }
            | Acknowledge
            | RecordReorg { .. }
            | AcknowledgeReorg
            | ReWitness { .. } => Err(OnChainTradeError::NotFilled),
        }
    }

    fn transition(
        &self,
        command: Self::Command,
        _jobs: &mut JobQueue<Self::Jobs>,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use OnChainTradeCommand::*;
        use OnChainTradeEvent::*;
        match command {
            Witness { .. } => Err(OnChainTradeError::AlreadyFilled),

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
                // A reorg invalidated this fill's block, so its position impact
                // must not be applied. Reject before the acknowledged guard: a
                // retried `AccountForDexTrade` job landing after `RecordReorg`
                // would otherwise complete the acknowledged marker while the
                // conductor resume path (keyed on `!is_acknowledged()`) skips
                // re-applying position impact -- leaving position and dedupe
                // state inconsistent with a fill invalidated on-chain.
                if self.is_reorged() {
                    return Err(OnChainTradeError::CannotAcknowledgeReorgedFill);
                }

                if self.is_acknowledged() {
                    return Err(OnChainTradeError::AlreadyAcknowledged);
                }

                Ok(vec![Acknowledged {
                    acknowledged_at: Utc::now(),
                }])
            }

            RecordReorg { reorg_depth } => {
                if self.is_reorged() {
                    return Err(OnChainTradeError::AlreadyReorged);
                }

                Ok(vec![Reorged {
                    reorg_depth,
                    reorged_at: Utc::now(),
                }])
            }

            AcknowledgeReorg => {
                if !self.is_reorged() {
                    return Err(OnChainTradeError::NotReorged);
                }

                if self.is_reorg_acknowledged() {
                    return Err(OnChainTradeError::AlreadyReorgAcknowledged);
                }

                Ok(vec![ReorgAcknowledged {
                    reorg_acknowledged_at: Utc::now(),
                }])
            }

            ReWitness {
                block_number,
                block_hash,
                block_timestamp,
            } => {
                // Only re-witness once the reversal is fully settled across both
                // aggregates. Re-witnessing earlier would clear the reorg markers
                // mid-reversal and desync the resume keying in `record_reorg`.
                if !self.is_reorg_acknowledged() {
                    return Err(OnChainTradeError::NotReorgAcknowledged);
                }

                Ok(vec![ReWitnessed {
                    block_number,
                    block_hash,
                    block_timestamp,
                    re_witnessed_at: Utc::now(),
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

    /// Whether a reorg has invalidated this fill's block. Once set, the trade is
    /// preserved (not deleted) and flagged reorged in the event log; reorg state
    /// is read from there and the `Position` aggregate, not from a view.
    pub(crate) fn is_reorged(&self) -> bool {
        self.reorged_at.is_some()
    }

    /// Whether the `Position` reversal for this reorg has completed -- the
    /// condition under which the reorg dedupe treats the reversal as fully
    /// applied across both aggregates (ADR 0012).
    pub(crate) fn is_reorg_acknowledged(&self) -> bool {
        self.reorg_acknowledged_at.is_some()
    }

    pub(crate) fn to_trade(&self, id: &OnChainTradeId) -> Trade {
        Trade {
            id: id.to_string(),
            filled_at: self.filled_at,
            venue: TradingVenue::Raindex,
            direction: self.direction,
            symbol: self.symbol.clone(),
            shares: FractionalShares::new(self.amount),
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
    #[error("Trade has already been recorded as reorged")]
    AlreadyReorged,
    #[error("Cannot acknowledge a fill whose block was reorged away")]
    CannotAcknowledgeReorgedFill,
    #[error("Cannot acknowledge a reorg for a trade that has not been reorged")]
    NotReorged,
    #[error("Trade's reorg has already been acknowledged")]
    AlreadyReorgAcknowledged,
    #[error("Cannot re-witness a fill whose reorg reversal is not yet acknowledged")]
    NotReorgAcknowledged,
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
        block_hash: Option<B256>,
        block_timestamp: DateTime<Utc>,
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
    /// Records that a reorg invalidated this fill's block. `reorg_depth` is
    /// how many blocks deep the reorg ran, kept for the audit trail.
    RecordReorg { reorg_depth: u64 },
    /// Marks the reorg's `Position` reversal as complete. Sent only after
    /// `Position::RecordReorg` succeeded, so the reorg dedupe can distinguish
    /// "reversal started" from "reversal finished" and resume a crashed
    /// reversal instead of skipping it (ADR 0012).
    AcknowledgeReorg,
    /// Re-witnesses a reorged fill on the new canonical block it was re-mined
    /// on. The same `(tx_hash, log_index)` re-confirmed with identical economic
    /// content (amount/direction/price) on a block the original reorg did not
    /// hold. Valid only once the reversal is fully acknowledged: it rewrites the
    /// block fields and clears the acknowledge/reorg markers so the fill can be
    /// re-acknowledged through the normal `Acknowledge` path, restoring the
    /// position to the fill's impact (the reverse-then-reapply reorg model).
    ReWitness {
        block_number: u64,
        block_hash: Option<B256>,
        block_timestamp: DateTime<Utc>,
    },
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
        #[serde(default)]
        block_hash: Option<B256>,
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
    Reorged {
        reorg_depth: u64,
        reorged_at: DateTime<Utc>,
    },
    ReorgAcknowledged {
        reorg_acknowledged_at: DateTime<Utc>,
    },
    /// The reorged fill was re-mined on a new canonical block. Resets the
    /// aggregate to a fresh post-witness state for that block: updates the block
    /// fields and clears `acknowledged_at`/`reorged_at`/`reorg_acknowledged_at`
    /// while preserving the unchanged fill content (`symbol`/`amount`/
    /// `direction`/`price_usdc`), so the fill can be re-acknowledged like a fresh
    /// witness.
    ReWitnessed {
        block_number: u64,
        #[serde(default)]
        block_hash: Option<B256>,
        block_timestamp: DateTime<Utc>,
        re_witnessed_at: DateTime<Utc>,
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
                    block_hash: block_hash_a,
                    block_timestamp: block_ts_a,
                    filled_at: fill_a,
                },
                Self::Filled {
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
                sym_a == sym_b
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
                Self::Reorged {
                    reorg_depth: depth_a,
                    reorged_at: at_a,
                },
                Self::Reorged {
                    reorg_depth: depth_b,
                    reorged_at: at_b,
                },
            ) => depth_a == depth_b && at_a == at_b,
            (
                Self::ReorgAcknowledged {
                    reorg_acknowledged_at: at_a,
                },
                Self::ReorgAcknowledged {
                    reorg_acknowledged_at: at_b,
                },
            ) => at_a == at_b,
            (
                Self::ReWitnessed {
                    block_number: block_num_a,
                    block_hash: block_hash_a,
                    block_timestamp: block_ts_a,
                    re_witnessed_at: at_a,
                },
                Self::ReWitnessed {
                    block_number: block_num_b,
                    block_hash: block_hash_b,
                    block_timestamp: block_ts_b,
                    re_witnessed_at: at_b,
                },
            ) => {
                block_num_a == block_num_b
                    && block_hash_a == block_hash_b
                    && block_ts_a == block_ts_b
                    && at_a == at_b
            }
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
            Self::Reorged { .. } => "OnChainTradeEvent::Reorged".to_string(),
            Self::ReorgAcknowledged { .. } => "OnChainTradeEvent::ReorgAcknowledged".to_string(),
            Self::ReWitnessed { .. } => "OnChainTradeEvent::ReWitnessed".to_string(),
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
    use alloy::primitives::b256;
    use st0x_event_sorcery::{LifecycleError, TestHarness, replay};

    use super::*;
    use st0x_float_macro::float;

    #[tokio::test]
    async fn witness_command_creates_filled_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let events = TestHarness::<OnChainTrade>::with()
            .given_no_previous_events()
            .when(OnChainTradeCommand::Witness {
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
    async fn witness_threads_block_hash_into_filled_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let block_hash =
            b256!("0xabababababababababababababababababababababababababababababababab");

        let events = TestHarness::<OnChainTrade>::with()
            .given_no_previous_events()
            .when(OnChainTradeCommand::Witness {
                symbol,
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
    async fn enrich_command_creates_enriched_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let pyth_price = PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: now,
        };

        let events = TestHarness::<OnChainTrade>::with()
            .given(vec![OnChainTradeEvent::Filled {
                symbol: symbol.clone(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
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

        let error = TestHarness::<OnChainTrade>::with()
            .given(vec![
                OnChainTradeEvent::Filled {
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

        let error = TestHarness::<OnChainTrade>::with()
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

        let error = TestHarness::<OnChainTrade>::with()
            .given(vec![OnChainTradeEvent::Filled {
                symbol,
                amount: float!("10.5"),
                direction: Direction::Buy,
                price_usdc: float!("150.25"),
                block_number: 12345,
                block_hash: None,
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

        let events = TestHarness::<OnChainTrade>::with()
            .given(vec![OnChainTradeEvent::Filled {
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

        let error = TestHarness::<OnChainTrade>::with()
            .given(vec![
                OnChainTradeEvent::Filled {
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
        let error = TestHarness::<OnChainTrade>::with()
            .given_no_previous_events()
            .when(OnChainTradeCommand::Acknowledge)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::NotFilled)
        ));
    }

    #[tokio::test]
    async fn record_reorg_marks_filled_trade_reorged() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let events = TestHarness::<OnChainTrade>::with()
            .given(vec![OnChainTradeEvent::Filled {
                symbol,
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
                block_timestamp: now,
                filled_at: now,
            }])
            .when(OnChainTradeCommand::RecordReorg { reorg_depth: 3 })
            .await
            .events();

        assert!(
            matches!(
                events.as_slice(),
                [OnChainTradeEvent::Reorged { reorg_depth: 3, .. }]
            ),
            "RecordReorg on a filled trade must emit Reorged carrying the depth; got {events:?}",
        );
    }

    #[tokio::test]
    async fn cannot_acknowledge_after_reorg() {
        // A reorg invalidated the fill, so a retried AccountForDexTrade job that
        // lands after RecordReorg must not complete the acknowledged marker --
        // that would desync position/dedupe state from an on-chain-invalid fill.
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let error = TestHarness::<OnChainTrade>::with()
            .given(vec![
                OnChainTradeEvent::Filled {
                    symbol,
                    amount: float!(10.5),
                    direction: Direction::Buy,
                    price_usdc: float!(150.25),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: now,
                },
                OnChainTradeEvent::Reorged {
                    reorg_depth: 3,
                    reorged_at: now,
                },
            ])
            .when(OnChainTradeCommand::Acknowledge)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::CannotAcknowledgeReorgedFill)
        ));
    }

    /// The reorg is append-only: marking a trade reorged must leave every
    /// original fill field intact so the reorged trade stays auditable in the
    /// event log with its original fill data.
    #[test]
    fn reorged_preserves_original_fill_data() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let trade = replay::<OnChainTrade>(vec![
            OnChainTradeEvent::Filled {
                symbol: symbol.clone(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
                block_timestamp: now,
                filled_at: now,
            },
            OnChainTradeEvent::Reorged {
                reorg_depth: 3,
                reorged_at: now,
            },
        ])
        .unwrap()
        .expect("replay must produce a live trade");

        assert!(trade.is_reorged());
        assert_eq!(trade.symbol, symbol);
        assert!(trade.amount.eq(float!(10.5)).unwrap());
        assert_eq!(trade.direction, Direction::Buy);
        assert_eq!(trade.block_number, Some(12345));
        assert_eq!(trade.filled_at, now);
    }

    /// Reorgs strike already-processed fills, so the marker must coexist with
    /// the acknowledgement rather than replace it.
    #[test]
    fn reorg_after_acknowledge_preserves_both_markers() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let trade = replay::<OnChainTrade>(vec![
            OnChainTradeEvent::Filled {
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
            OnChainTradeEvent::Reorged {
                reorg_depth: 1,
                reorged_at: now,
            },
        ])
        .unwrap()
        .expect("replay must produce a live trade");

        assert!(trade.is_reorged());
        assert!(trade.is_acknowledged());
    }

    /// Drives the command path end-to-end: an already-acknowledged fill struck
    /// by a reorg must emit `Reorged`, keep its acknowledgement, and then reject
    /// any retried `Acknowledge` with `CannotAcknowledgeReorgedFill` -- so a
    /// re-delivered `AccountForDexTrade` job can never complete the marker on a
    /// fill whose block was reorged away. The replay-only sibling is
    /// `reorg_after_acknowledge_preserves_both_markers`; this one threads the
    /// real events emitted by each command handler into the next step.
    #[tokio::test]
    async fn record_reorg_after_acknowledge_marks_reorged_and_blocks_reacknowledge() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let filled = OnChainTradeEvent::Filled {
            symbol,
            amount: float!(10.5),
            direction: Direction::Buy,
            price_usdc: float!(150.25),
            block_number: 12345,
            block_hash: None,
            block_timestamp: now,
            filled_at: now,
        };

        // Acknowledge the witnessed fill through the command handler.
        let acknowledge_events = TestHarness::<OnChainTrade>::with()
            .given(vec![filled.clone()])
            .when(OnChainTradeCommand::Acknowledge)
            .await
            .events();
        let [acknowledged] = acknowledge_events.as_slice() else {
            panic!("Acknowledge must emit a single marker; got {acknowledge_events:?}");
        };
        assert!(matches!(
            acknowledged,
            OnChainTradeEvent::Acknowledged { .. }
        ));

        // RecordReorg the now-acknowledged fill through the command handler.
        let reorg_events = TestHarness::<OnChainTrade>::with()
            .given(vec![filled.clone(), acknowledged.clone()])
            .when(OnChainTradeCommand::RecordReorg { reorg_depth: 4 })
            .await
            .events();
        let [reorged] = reorg_events.as_slice() else {
            panic!("RecordReorg must emit a single marker; got {reorg_events:?}");
        };
        assert!(matches!(
            reorged,
            OnChainTradeEvent::Reorged { reorg_depth: 4, .. }
        ));

        // (a) The live aggregate carries both markers after the command sequence.
        let trade =
            replay::<OnChainTrade>(vec![filled.clone(), acknowledged.clone(), reorged.clone()])
                .unwrap()
                .expect("replay must produce a live trade");
        assert!(trade.is_reorged());
        assert!(trade.is_acknowledged());

        // (b) A retried Acknowledge after the reorg is rejected.
        let error = TestHarness::<OnChainTrade>::with()
            .given(vec![filled, acknowledged.clone(), reorged.clone()])
            .when(OnChainTradeCommand::Acknowledge)
            .await
            .then_expect_error();
        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::CannotAcknowledgeReorgedFill)
        ));
    }

    #[tokio::test]
    async fn cannot_reorg_twice() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let error = TestHarness::<OnChainTrade>::with()
            .given(vec![
                OnChainTradeEvent::Filled {
                    symbol,
                    amount: float!(10.5),
                    direction: Direction::Buy,
                    price_usdc: float!(150.25),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: now,
                },
                OnChainTradeEvent::Reorged {
                    reorg_depth: 2,
                    reorged_at: now,
                },
            ])
            .when(OnChainTradeCommand::RecordReorg { reorg_depth: 5 })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::AlreadyReorged)
        ));
    }

    #[tokio::test]
    async fn cannot_reorg_unfilled_trade() {
        let error = TestHarness::<OnChainTrade>::with()
            .given_no_previous_events()
            .when(OnChainTradeCommand::RecordReorg { reorg_depth: 1 })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::NotFilled)
        ));
    }

    fn filled_then_reorged(now: DateTime<Utc>) -> Vec<OnChainTradeEvent> {
        vec![
            OnChainTradeEvent::Filled {
                symbol: Symbol::new("AAPL").unwrap(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
                block_timestamp: now,
                filled_at: now,
            },
            OnChainTradeEvent::Reorged {
                reorg_depth: 3,
                reorged_at: now,
            },
        ]
    }

    #[tokio::test]
    async fn acknowledge_reorg_marks_reversal_complete() {
        let now = Utc::now();

        let events = TestHarness::<OnChainTrade>::with()
            .given(filled_then_reorged(now))
            .when(OnChainTradeCommand::AcknowledgeReorg)
            .await
            .events();

        assert!(
            matches!(
                events.as_slice(),
                [OnChainTradeEvent::ReorgAcknowledged { .. }]
            ),
            "AcknowledgeReorg on a reorged trade must emit the marker; got {events:?}",
        );
    }

    /// The reorg-ack marker is what `record_reorg` reads to know the `Position`
    /// reversal completed; replay must surface it.
    #[test]
    fn reorg_acknowledged_sets_marker() {
        let now = Utc::now();
        let mut events = filled_then_reorged(now);
        events.push(OnChainTradeEvent::ReorgAcknowledged {
            reorg_acknowledged_at: now,
        });

        let trade = replay::<OnChainTrade>(events)
            .unwrap()
            .expect("replay must produce a live trade");

        assert!(trade.is_reorged());
        assert!(trade.is_reorg_acknowledged());
    }

    #[tokio::test]
    async fn cannot_acknowledge_reorg_before_reorged() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let error = TestHarness::<OnChainTrade>::with()
            .given(vec![OnChainTradeEvent::Filled {
                symbol,
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
                block_timestamp: now,
                filled_at: now,
            }])
            .when(OnChainTradeCommand::AcknowledgeReorg)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::NotReorged)
        ));
    }

    #[tokio::test]
    async fn cannot_acknowledge_reorg_twice() {
        let now = Utc::now();
        let mut events = filled_then_reorged(now);
        events.push(OnChainTradeEvent::ReorgAcknowledged {
            reorg_acknowledged_at: now,
        });

        let error = TestHarness::<OnChainTrade>::with()
            .given(events)
            .when(OnChainTradeCommand::AcknowledgeReorg)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::AlreadyReorgAcknowledged)
        ));
    }

    fn filled_acknowledged_reorged_acknowledged(now: DateTime<Utc>) -> Vec<OnChainTradeEvent> {
        vec![
            OnChainTradeEvent::Filled {
                symbol: Symbol::new("AAPL").unwrap(),
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: Some(b256!(
                    "0xabababababababababababababababababababababababababababababababab"
                )),
                block_timestamp: now,
                filled_at: now,
            },
            OnChainTradeEvent::Acknowledged {
                acknowledged_at: now,
            },
            OnChainTradeEvent::Reorged {
                reorg_depth: 3,
                reorged_at: now,
            },
            OnChainTradeEvent::ReorgAcknowledged {
                reorg_acknowledged_at: now,
            },
        ]
    }

    #[tokio::test]
    async fn re_witness_after_reorg_acknowledged_emits_re_witnessed() {
        let now = Utc::now();
        let new_block_hash =
            b256!("0xbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc");

        let events = TestHarness::<OnChainTrade>::with()
            .given(filled_acknowledged_reorged_acknowledged(now))
            .when(OnChainTradeCommand::ReWitness {
                block_number: 99999,
                block_hash: Some(new_block_hash),
                block_timestamp: now,
            })
            .await
            .events();

        let [
            OnChainTradeEvent::ReWitnessed {
                block_number,
                block_hash,
                ..
            },
        ] = events.as_slice()
        else {
            panic!("ReWitness on a reorg-acknowledged trade must emit ReWitnessed; got {events:?}");
        };
        assert_eq!(*block_number, 99999);
        assert_eq!(*block_hash, Some(new_block_hash));
    }

    #[tokio::test]
    async fn cannot_re_witness_before_reorg_acknowledged() {
        // A fill reorged but whose reversal is not yet acknowledged must not be
        // re-witnessed -- that would clear the reorg markers mid-reversal and
        // desync the `record_reorg` resume keying.
        let now = Utc::now();

        let error = TestHarness::<OnChainTrade>::with()
            .given(filled_then_reorged(now))
            .when(OnChainTradeCommand::ReWitness {
                block_number: 99999,
                block_hash: None,
                block_timestamp: now,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::NotReorgAcknowledged)
        ));
    }

    #[tokio::test]
    async fn cannot_re_witness_a_live_fill() {
        // A freshly witnessed fill that was never reorged cannot be re-witnessed.
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let error = TestHarness::<OnChainTrade>::with()
            .given(vec![OnChainTradeEvent::Filled {
                symbol,
                amount: float!(10.5),
                direction: Direction::Buy,
                price_usdc: float!(150.25),
                block_number: 12345,
                block_hash: None,
                block_timestamp: now,
                filled_at: now,
            }])
            .when(OnChainTradeCommand::ReWitness {
                block_number: 99999,
                block_hash: None,
                block_timestamp: now,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(OnChainTradeError::NotReorgAcknowledged)
        ));
    }

    /// `ReWitnessed` resets the aggregate to a fresh post-witness state: the
    /// block fields advance to the new canonical block, all three lifecycle
    /// markers clear, and the unchanged fill content survives.
    #[test]
    fn re_witnessed_resets_to_fresh_post_witness_state() {
        let now = Utc::now();
        let symbol = Symbol::new("AAPL").unwrap();
        let new_block_hash =
            b256!("0xbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc");

        let mut events = filled_acknowledged_reorged_acknowledged(now);
        events.push(OnChainTradeEvent::ReWitnessed {
            block_number: 99999,
            block_hash: Some(new_block_hash),
            block_timestamp: now,
            re_witnessed_at: now,
        });

        let trade = replay::<OnChainTrade>(events)
            .unwrap()
            .expect("replay must produce a live trade");

        assert!(
            !trade.is_acknowledged(),
            "re-witness must clear the acknowledge marker",
        );
        assert!(
            !trade.is_reorged(),
            "re-witness must clear the reorged marker"
        );
        assert!(
            !trade.is_reorg_acknowledged(),
            "re-witness must clear the reorg-ack marker",
        );
        assert_eq!(trade.block_number, Some(99999));
        assert_eq!(trade.block_hash, Some(new_block_hash));
        // The fill content is unchanged: the same economic fill on a new block.
        assert_eq!(trade.symbol, symbol);
        assert!(trade.amount.eq(float!(10.5)).unwrap());
        assert_eq!(trade.direction, Direction::Buy);
        assert!(trade.price_usdc.eq(float!(150.25)).unwrap());
    }

    /// The reverse-then-reapply model relies on a re-witnessed fill accepting
    /// `Acknowledge` again, so the position can be returned to the fill's impact.
    #[tokio::test]
    async fn acknowledge_succeeds_after_re_witness() {
        let now = Utc::now();

        let mut events = filled_acknowledged_reorged_acknowledged(now);
        events.push(OnChainTradeEvent::ReWitnessed {
            block_number: 99999,
            block_hash: None,
            block_timestamp: now,
            re_witnessed_at: now,
        });

        let acknowledge_events = TestHarness::<OnChainTrade>::with()
            .given(events)
            .when(OnChainTradeCommand::Acknowledge)
            .await
            .events();

        assert!(
            matches!(
                acknowledge_events.as_slice(),
                [OnChainTradeEvent::Acknowledged { .. }]
            ),
            "a re-witnessed fill must accept Acknowledge again; got {acknowledge_events:?}",
        );
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
        assert!(trade.is_enriched());
    }

    #[tokio::test]
    async fn cannot_witness_twice_when_filled() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let error = TestHarness::<OnChainTrade>::with()
            .given(vec![OnChainTradeEvent::Filled {
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

        let error = TestHarness::<OnChainTrade>::with()
            .given(vec![
                OnChainTradeEvent::Filled {
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
        assert!(!trade.is_enriched());
    }

    #[test]
    fn filled_carries_block_hash_into_live_state() {
        let block_hash =
            b256!("0xabababababababababababababababababababababababababababababababab");
        let now = Utc::now();

        let trade = replay::<OnChainTrade>(vec![OnChainTradeEvent::Filled {
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
