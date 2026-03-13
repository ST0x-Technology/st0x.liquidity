//! Lightweight envelope used to represent an event included on-chain.
//!
//! [`ChainIncluded`] carries just enough metadata to identify and process
//! an onchain trade event. It is serialized into
//! [`AccountForDexTrade`] payloads and persisted for processing.
//!
//! [`AccountForDexTrade`]: super::trade_accountant::AccountForDexTrade

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

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, LogData, fixed_bytes};
    use alloy::rpc::types::Log;

    use super::*;

    fn valid_log() -> Log {
        Log {
            inner: alloy::primitives::Log {
                address: Address::ZERO,
                data: LogData::empty(),
            },
            block_hash: None,
            block_number: Some(42),
            block_timestamp: Some(1_700_000_000),
            transaction_hash: Some(fixed_bytes!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )),
            transaction_index: None,
            log_index: Some(7),
            removed: false,
        }
    }

    #[test]
    fn from_log_extracts_all_fields() {
        let log = valid_log();
        let included = ChainIncluded::from_log("test_event", &log).unwrap();

        assert_eq!(included.event, "test_event");
        assert_eq!(included.tx_hash, log.transaction_hash.unwrap());
        assert_eq!(included.log_index, 7);
        assert_eq!(included.block_number, 42);
        assert!(
            included.block_timestamp.is_some(),
            "Should parse valid timestamp"
        );
    }

    #[test]
    fn from_log_handles_missing_timestamp() {
        let mut log = valid_log();
        log.block_timestamp = None;

        let included = ChainIncluded::from_log("event", &log).unwrap();

        assert!(
            included.block_timestamp.is_none(),
            "Missing timestamp should produce None, not error"
        );
    }

    #[test]
    fn from_log_fails_without_tx_hash() {
        let mut log = valid_log();
        log.transaction_hash = None;

        let error = ChainIncluded::from_log("event", &log).unwrap_err();

        assert!(
            matches!(
                error,
                BlockInclusionError::MissingLogField(RequiredField::TxHash)
            ),
            "Expected MissingLogField(TxHash), got: {error}"
        );
    }

    #[test]
    fn from_log_fails_without_log_index() {
        let mut log = valid_log();
        log.log_index = None;

        let error = ChainIncluded::from_log("event", &log).unwrap_err();

        assert!(
            matches!(
                error,
                BlockInclusionError::MissingLogField(RequiredField::LogIndex)
            ),
            "Expected MissingLogField(LogIndex), got: {error}"
        );
    }

    #[test]
    fn from_log_fails_without_block_number() {
        let mut log = valid_log();
        log.block_number = None;

        let error = ChainIncluded::from_log("event", &log).unwrap_err();

        assert!(
            matches!(
                error,
                BlockInclusionError::MissingLogField(RequiredField::BlockNumber)
            ),
            "Expected MissingLogField(BlockNumber), got: {error}"
        );
    }

    #[test]
    fn timestamp_parsed_as_utc_datetime() {
        let log = valid_log();
        let included = ChainIncluded::from_log("event", &log).unwrap();

        let timestamp = included.block_timestamp.unwrap();
        assert_eq!(timestamp.timestamp(), 1_700_000_000);
    }

    #[test]
    fn required_field_display() {
        assert_eq!(RequiredField::TxHash.to_string(), "transaction_hash");
        assert_eq!(RequiredField::LogIndex.to_string(), "log_index");
        assert_eq!(RequiredField::BlockNumber.to_string(), "block_number");
    }
}
