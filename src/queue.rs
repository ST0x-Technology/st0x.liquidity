//! Lightweight event envelope used by the apalis job pipeline.
//!
//! [`QueuedEvent`] carries just enough metadata to identify and process
//! an onchain trade event. It is serialized into [`OrderFillJob`] payloads
//! and stored in apalis's `Jobs` table

use alloy::primitives::TxHash;
use alloy::rpc::types::Log;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::num::TryFromIntError;

use crate::onchain::trade::TradeEvent;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueuedEvent {
    pub(crate) tx_hash: TxHash,
    pub(crate) log_index: u64,
    pub(crate) block_number: u64,
    pub(crate) event: TradeEvent,
    pub(crate) block_timestamp: Option<DateTime<Utc>>,
}

impl QueuedEvent {
    /// Constructs a [`QueuedEvent`] from a decoded trade event and its
    /// log metadata, without writing to the database. Used by the apalis
    /// event monitor to build job payloads directly.
    pub(crate) fn from_log(event: TradeEvent, log: &Log) -> Result<Self, EventQueueError> {
        let tx_hash = log
            .transaction_hash
            .ok_or(EventQueueError::MissingLogField("transaction_hash"))?;

        let log_index = log
            .log_index
            .ok_or(EventQueueError::MissingLogField("log_index"))?;

        let block_number = log
            .block_number
            .ok_or(EventQueueError::MissingLogField("block_number"))?;

        let block_timestamp = log.block_timestamp.and_then(|ts| {
            let ts_i64 = i64::try_from(ts).ok()?;
            DateTime::from_timestamp(ts_i64, 0)
        });

        Ok(Self {
            tx_hash,
            log_index,
            block_number,
            event,
            block_timestamp,
        })
    }
}

/// Event queue errors.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EventQueueError {
    #[error("Log missing required field: {0}")]
    MissingLogField(&'static str),
    #[error("Integer conversion error: {0}")]
    IntConversion(#[from] TryFromIntError),
    #[error("Event serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Invalid tx_hash format: {0}")]
    InvalidTxHash(#[from] alloy::hex::FromHexError),
}
