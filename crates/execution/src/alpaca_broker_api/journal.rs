//! Alpaca Broker API journal types for transferring securities
//! between accounts under the same firm.

use chrono::NaiveDate;
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
    #[serde(rename = "qty", serialize_with = "serialize_positive_shares")]
    pub(super) quantity: Positive<FractionalShares>,
}

// serde's serialize_with requires the field to be passed by reference
#[allow(clippy::trivially_copy_pass_by_ref)]
fn serialize_positive_shares<S>(
    shares: &Positive<FractionalShares>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&shares.inner().inner().to_string())
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
            quantity,
        }
    }
}

/// Response from the Alpaca Broker API when creating a journal.
#[derive(Debug, Deserialize)]
pub struct JournalResponse {
    pub id: Uuid,
    pub status: JournalStatus,
    pub symbol: Symbol,
    #[serde(
        rename = "qty",
        deserialize_with = "deserialize_positive_fractional_shares"
    )]
    pub quantity: Positive<FractionalShares>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    pub price: Option<Decimal>,
    pub from_account: AlpacaAccountId,
    pub to_account: AlpacaAccountId,
    pub settle_date: Option<NaiveDate>,
    pub system_date: Option<NaiveDate>,
    pub description: Option<String>,
}

fn deserialize_positive_fractional_shares<'de, D>(
    deserializer: D,
) -> Result<Positive<FractionalShares>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let string = String::deserialize(deserializer)?;
    let decimal: Decimal = string.parse().map_err(serde::de::Error::custom)?;
    let shares = FractionalShares::new(decimal);
    Positive::new(shares).map_err(serde::de::Error::custom)
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
    use rust_decimal_macros::dec;
    use uuid::uuid;

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

    fn journal_json(qty: &str) -> serde_json::Value {
        serde_json::json!({
            "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "status": "pending",
            "symbol": "AAPL",
            "qty": qty,
            "from_account": "904837e3-3b76-47ec-b432-046db621571b",
            "to_account": "11111111-2222-3333-4444-555555555555"
        })
    }

    #[test]
    fn deserialize_quantity_rejects_zero() {
        let json = journal_json("0");
        let result = serde_json::from_value::<JournalResponse>(json);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_quantity_rejects_negative() {
        let json = journal_json("-5.0");
        let result = serde_json::from_value::<JournalResponse>(json);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_quantity_accepts_positive() {
        let json = journal_json("10.5");
        let response: JournalResponse = serde_json::from_value(json).unwrap();
        assert_eq!(
            response.quantity,
            Positive::new(FractionalShares::new(dec!(10.5))).unwrap()
        );
    }

    #[test]
    fn journal_request_serializes_quantity_as_string() {
        let from = AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));
        let to = AlpacaAccountId::new(uuid!("11111111-2222-3333-4444-555555555555"));
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = Positive::new(FractionalShares::new(dec!(10.5))).unwrap();

        let request = JournalRequest::security(from, to, symbol, quantity);
        let json = serde_json::to_value(&request).unwrap();

        assert_eq!(json["qty"], "10.5");
    }

    #[test]
    fn deserialize_dates_as_naive_date() {
        let json = serde_json::json!({
            "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "status": "executed",
            "symbol": "AAPL",
            "qty": "10",
            "from_account": "904837e3-3b76-47ec-b432-046db621571b",
            "to_account": "11111111-2222-3333-4444-555555555555",
            "settle_date": "2026-02-28",
            "system_date": "2026-02-26"
        });

        let response: JournalResponse = serde_json::from_value(json).unwrap();

        assert_eq!(response.settle_date, NaiveDate::from_ymd_opt(2026, 2, 28));
        assert_eq!(response.system_date, NaiveDate::from_ymd_opt(2026, 2, 26));
    }
}
