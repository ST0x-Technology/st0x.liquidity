//! Alpaca Broker API journal types for transferring securities
//! between accounts under the same firm.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::auth::AlpacaAccountId;
use crate::{FractionalShares, Positive, Symbol};

/// Type of journal entry in the Alpaca Broker API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) enum JournalEntryType {
    /// Journal of securities (stock positions).
    Jnls,
}

/// Request body for creating a security journal (JNLS) between accounts.
#[derive(Debug, Serialize)]
pub(super) struct JournalRequest {
    pub(super) from_account: AlpacaAccountId,
    pub(super) to_account: AlpacaAccountId,
    pub(super) entry_type: JournalEntryType,
    pub(super) symbol: Symbol,
    #[serde(rename = "qty")]
    pub(super) quantity: String,
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
            entry_type: JournalEntryType::Jnls,
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
        use JournalStatus::*;

        match self {
            Queued => write!(f, "queued"),
            SentToClearing => write!(f, "sent_to_clearing"),
            Pending => write!(f, "pending"),
            Executed => write!(f, "executed"),
            Rejected => write!(f, "rejected"),
            Canceled => write!(f, "canceled"),
            Refused => write!(f, "refused"),
            Deleted => write!(f, "deleted"),
            Correct => write!(f, "correct"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_status_display_matches_serde_names() {
        assert_eq!(JournalStatus::Queued.to_string(), "queued");
        assert_eq!(
            JournalStatus::SentToClearing.to_string(),
            "sent_to_clearing"
        );
        assert_eq!(JournalStatus::Pending.to_string(), "pending");
        assert_eq!(JournalStatus::Executed.to_string(), "executed");
        assert_eq!(JournalStatus::Rejected.to_string(), "rejected");
        assert_eq!(JournalStatus::Canceled.to_string(), "canceled");
        assert_eq!(JournalStatus::Refused.to_string(), "refused");
        assert_eq!(JournalStatus::Deleted.to_string(), "deleted");
        assert_eq!(JournalStatus::Correct.to_string(), "correct");
    }

    #[test]
    fn journal_entry_type_serializes_as_screaming_snake_case() {
        let json = serde_json::to_string(&JournalEntryType::Jnls).unwrap();
        assert_eq!(json, "\"JNLS\"");
    }
}
