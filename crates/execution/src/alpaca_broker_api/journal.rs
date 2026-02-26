//! Alpaca Broker API journal types for transferring securities
//! between accounts under the same firm.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::auth::AlpacaAccountId;
use crate::Symbol;

/// Request body for creating a security journal (JNLS) between accounts.
#[derive(Debug, Serialize)]
pub(super) struct JournalRequest {
    pub from_account: AlpacaAccountId,
    pub to_account: AlpacaAccountId,
    pub entry_type: &'static str,
    pub symbol: Symbol,
    pub qty: String,
}

/// Response from the Alpaca Broker API when creating a journal.
#[derive(Debug, Deserialize)]
pub struct JournalResponse {
    pub id: Uuid,
    pub status: JournalStatus,
    pub symbol: String,
    pub qty: String,
    pub price: Option<String>,
    pub from_account: Uuid,
    pub to_account: Uuid,
    pub settle_date: Option<String>,
    pub system_date: Option<String>,
    pub description: Option<String>,
}

/// Status of an Alpaca journal entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalStatus {
    Queued,
    SentToClearing,
    Pending,
    Executed,
    Rejected,
    Canceled,
    Refused,
    Deleted,
    Correct,
}

impl std::fmt::Display for JournalStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queued => write!(f, "queued"),
            Self::SentToClearing => write!(f, "sent_to_clearing"),
            Self::Pending => write!(f, "pending"),
            Self::Executed => write!(f, "executed"),
            Self::Rejected => write!(f, "rejected"),
            Self::Canceled => write!(f, "canceled"),
            Self::Refused => write!(f, "refused"),
            Self::Deleted => write!(f, "deleted"),
            Self::Correct => write!(f, "correct"),
        }
    }
}
