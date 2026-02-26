//! Alpaca Broker API journal types for transferring securities
//! between accounts under the same firm.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::auth::AlpacaAccountId;
use crate::{FractionalShares, Positive, Symbol};

/// Request body for creating a security journal (JNLS) between accounts.
#[derive(Debug, Serialize)]
pub(super) struct JournalRequest {
    pub from_account: AlpacaAccountId,
    pub to_account: AlpacaAccountId,
    pub entry_type: &'static str,
    pub symbol: Symbol,
    #[serde(rename = "qty")]
    pub quantity: String,
}

impl JournalRequest {
    pub(super) fn security(
        from_account: AlpacaAccountId,
        to_account: AlpacaAccountId,
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
    ) -> Self {
        Self {
            from_account,
            to_account,
            entry_type: "JNLS",
            symbol,
            quantity: quantity.to_string(),
        }
    }
}

/// Response from the Alpaca Broker API when creating a journal.
#[derive(Debug, Deserialize)]
pub struct JournalResponse {
    pub id: Uuid,
    pub status: JournalStatus,
    pub symbol: Symbol,
    #[serde(rename = "qty", deserialize_with = "deserialize_fractional_shares")]
    pub quantity: FractionalShares,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    pub price: Option<Decimal>,
    pub from_account: AlpacaAccountId,
    pub to_account: AlpacaAccountId,
    pub settle_date: Option<String>,
    pub system_date: Option<String>,
    pub description: Option<String>,
}

fn deserialize_fractional_shares<'de, D>(deserializer: D) -> Result<FractionalShares, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let string = String::deserialize(deserializer)?;
    let decimal: Decimal = string.parse().map_err(serde::de::Error::custom)?;
    Ok(FractionalShares::new(decimal))
}

fn deserialize_optional_decimal<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let maybe_string: Option<String> = Option::deserialize(deserializer)?;
    maybe_string
        .map(|string| string.parse().map_err(serde::de::Error::custom))
        .transpose()
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
