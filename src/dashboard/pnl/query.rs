use std::collections::BTreeSet;

use chrono::{DateTime, Days, Utc};
use serde::Deserialize;

use super::parsing::{is_safe_symbol, parse_query_datetime};

#[derive(Debug, thiserror::Error)]
pub(crate) enum PnlError {
    #[error("invalid {field}: {value}")]
    InvalidDate { field: &'static str, value: String },
    #[error("failed to parse persisted PnL payload at row {rowid} ({aggregate_type}/{event_type})")]
    InvalidPayload {
        rowid: i64,
        aggregate_type: String,
        event_type: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to load PnL rows: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlQuery {
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
    pub(crate) symbol: Option<String>,
    pub(crate) from_date: Option<String>,
    pub(crate) to_date: Option<String>,
    pub(crate) market_session_filter: Option<PnlMarketSessionFilter>,
    pub(crate) counter_trading_filter: Option<PnlCounterTradingFilter>,
}

impl PnlQuery {
    pub(crate) fn normalized_limit(&self) -> usize {
        self.limit.unwrap_or(100).min(5_000)
    }

    pub(crate) fn normalized_offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }

    pub(crate) fn activity_after(&self) -> Result<Option<DateTime<Utc>>, PnlError> {
        self.from_date
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| parse_query_datetime(value, "fromDate"))
            .transpose()
    }

    pub(crate) fn activity_until(&self) -> Result<Option<DateTime<Utc>>, PnlError> {
        let Some(value) = self
            .to_date
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(None);
        };

        let parsed = parse_query_datetime(value, "toDate")?;
        let Some(date_value) = self.to_date.as_deref().filter(|value| value.len() == 10) else {
            return Ok(Some(parsed));
        };

        let until = parsed
            .date_naive()
            .checked_add_days(Days::new(1))
            .ok_or_else(|| PnlError::InvalidDate {
                field: "toDate",
                value: date_value.to_owned(),
            })?;
        Ok(Some(DateTime::from_naive_utc_and_offset(
            until
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| PnlError::InvalidDate {
                    field: "toDate",
                    value: date_value.to_owned(),
                })?,
            Utc,
        )))
    }

    pub(crate) fn symbol_filter(&self, warnings: &mut Vec<String>) -> BTreeSet<String> {
        let Some(raw) = &self.symbol else {
            return BTreeSet::new();
        };

        let mut symbols = BTreeSet::new();
        let mut invalid = Vec::new();
        for symbol in raw
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if is_safe_symbol(symbol) {
                symbols.insert(symbol.to_owned());
            } else {
                invalid.push(symbol.to_owned());
            }
        }

        if !invalid.is_empty() {
            warnings.push(format!(
                "Skipped {} invalid symbol filters in backend PnL query: {}",
                invalid.len(),
                invalid.join(", ")
            ));
        }

        symbols
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PnlMarketSessionFilter {
    All,
    Pre,
    Rth,
    Post,
    Overnight,
    Weekend,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PnlCounterTradingFilter {
    All,
    CounterTradingActive,
    CounterTradingInactive,
}
