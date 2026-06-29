//! Lightweight envelope used to represent an event included on-chain.
//!
//! [`EmittedOnChain`] carries just enough metadata to identify and process
//! an onchain trade event. It is serialized into
//! [`AccountForDexTrade`] payloads and persisted for processing.
//!
//! [`AccountForDexTrade`]: super::trade_accountant::AccountForDexTrade

use alloy::primitives::{B256, TxHash};
use alloy::rpc::types::Log;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::num::TryFromIntError;

/// Wraps an arbitrary event type with metadata about the exact point
/// at which the event was included in the ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EmittedOnChain<Event> {
    pub(crate) event: Event,
    pub(crate) tx_hash: TxHash,
    pub(crate) log_index: u64,
    pub(crate) block_number: u64,
    /// Hash of the block this event was included in. Identifies which fork
    /// a confirmed `(tx_hash, log_index)` came from, so a later replay against
    /// a different fork can be detected as a reorg rather than mistaken for a
    /// duplicate. `None` when the source log carried no block hash (pending
    /// logs, reconstructed logs).
    ///
    /// `#[serde(default)]` so apalis jobs serialized before this field existed
    /// (in-flight `AccountForDexTrade` payloads during a deploy) still
    /// deserialize -- they predate fork tracking, so `None` is correct for them.
    #[serde(default)]
    pub(crate) block_hash: Option<B256>,
    pub(crate) block_timestamp: Option<DateTime<Utc>>,
}

impl<Event> EmittedOnChain<Event> {
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
            block_hash: log.block_hash,
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

/// Required log fields for constructing an [`EmittedOnChain`].
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
    use alloy::primitives::{Address, LogData, b256, fixed_bytes};
    use alloy::rpc::types::Log;

    use super::*;

    fn valid_log() -> Log {
        Log {
            inner: alloy::primitives::Log {
                address: Address::ZERO,
                data: LogData::empty(),
            },
            block_hash: Some(b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            )),
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
        let included = EmittedOnChain::from_log("test_event", &log).unwrap();

        assert_eq!(included.event, "test_event");
        assert_eq!(included.tx_hash, log.transaction_hash.unwrap());
        assert_eq!(included.log_index, 7);
        assert_eq!(included.block_number, 42);
        assert_eq!(included.block_hash, log.block_hash);
        assert_eq!(
            included.block_timestamp,
            DateTime::from_timestamp(1_700_000_000, 0),
        );
    }

    #[test]
    fn from_log_handles_missing_block_hash() {
        let mut log = valid_log();
        log.block_hash = None;

        let included = EmittedOnChain::from_log("event", &log).unwrap();

        assert_eq!(included.block_hash, None);
    }

    #[test]
    fn from_log_handles_missing_timestamp() {
        let mut log = valid_log();
        log.block_timestamp = None;

        let included = EmittedOnChain::from_log("event", &log).unwrap();

        assert_eq!(included.block_timestamp, None);
    }

    #[test]
    fn from_log_fails_without_tx_hash() {
        let mut log = valid_log();
        log.transaction_hash = None;

        let error = EmittedOnChain::from_log("event", &log).unwrap_err();

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

        let error = EmittedOnChain::from_log("event", &log).unwrap_err();

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

        let error = EmittedOnChain::from_log("event", &log).unwrap_err();

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
        let included = EmittedOnChain::from_log("event", &log).unwrap();

        let timestamp = included.block_timestamp.unwrap();
        assert_eq!(timestamp.timestamp(), 1_700_000_000);
    }

    #[test]
    fn required_field_display() {
        assert_eq!(RequiredField::TxHash.to_string(), "transaction_hash");
        assert_eq!(RequiredField::LogIndex.to_string(), "log_index");
        assert_eq!(RequiredField::BlockNumber.to_string(), "block_number");
    }

    /// Apalis jobs serialized before `block_hash` existed (in-flight
    /// `AccountForDexTrade` payloads during a deploy) must still deserialize,
    /// with the absent field defaulting to `None` rather than failing the job.
    #[test]
    fn emitted_on_chain_without_block_hash_deserializes_to_none() {
        let json = serde_json::json!({
            "event": null,
            "tx_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "log_index": 7,
            "block_number": 42,
            "block_timestamp": null,
            // block_hash intentionally absent
        });

        let parsed: EmittedOnChain<()> = serde_json::from_value(json).unwrap();

        assert_eq!(parsed.block_hash, None);
    }
}
