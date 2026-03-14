//! Lightweight envelope used to represent an event included on-chain.
//!
//! [`ChainIncluded`] carries just enough metadata to identify and process
//! an onchain trade event. It is serialized into [`OrderFillJob`] payloads
//! and stored in apalis's `Jobs` table

use alloy::primitives::TxHash;
use alloy::rpc::types::Log;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::num::TryFromIntError;

/// Wraps an arbitrary event type with metadata about the exact point
/// at which the event was included in the ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ChainIncluded<Event> {
    pub(crate) event: Event,
    pub(crate) tx_hash: TxHash,
    pub(crate) log_index: u64,
    pub(crate) block_number: u64,
    pub(crate) block_timestamp: Option<DateTime<Utc>>,
}

impl<Event> ChainIncluded<Event> {
    /// Extracts block inclusion metadata from an RPC log and pairs it
    /// with the already-decoded event.
    pub(crate) fn from_log(event: Event, log: &Log) -> Result<Self, BlockInclusionError> {
        use BlockInclusionError::MissingLogField;

        let tx_hash = log
            .transaction_hash
            .ok_or(MissingLogField(RequiredField::TxHash))?;

        let log_index = log
            .log_index
            .ok_or(MissingLogField(RequiredField::LogIndex))?;

        let block_number = log
            .block_number
            .ok_or(MissingLogField(RequiredField::BlockNumber))?;

        let block_timestamp = log.block_timestamp.and_then(|ts| {
            let ts_i64 = i64::try_from(ts).ok()?;
            DateTime::from_timestamp(ts_i64, 0)
        });

        Ok(Self {
            event,
            tx_hash,
            log_index,
            block_number,
            block_timestamp,
        })
    }
}

/// Errors when extracting block inclusion metadata from a log.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BlockInclusionError {
    #[error("Log missing required field: {0}")]
    MissingLogField(RequiredField),
    #[error("Integer conversion error: {0}")]
    IntConversion(#[from] TryFromIntError),
    #[error("Event serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Invalid tx_hash format: {0}")]
    InvalidTxHash(#[from] alloy::hex::FromHexError),
}

/// Required log fields for constructing a [`ChainIncluded`].
#[derive(Debug)]
pub(crate) enum RequiredField {
    TxHash,
    LogIndex,
    BlockNumber,
}

impl fmt::Display for RequiredField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TxHash => write!(formatter, "transaction_hash"),
            Self::LogIndex => write!(formatter, "log_index"),
            Self::BlockNumber => write!(formatter, "block_number"),
        }
    }
}

// TODO: bring over tests
