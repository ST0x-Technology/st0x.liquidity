use std::collections::BTreeSet;

use chrono::{DateTime, Datelike, Days, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use serde::Deserialize;

use super::parsing::is_safe_symbol;

const ALPACA_ACTIVITY_FETCH_PADDING_DAYS: i64 = 7;

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
    #[error(
        "failed to parse persisted financial field {field} at row {rowid} ({aggregate_type}/{event_type}): {value} ({parse_error})"
    )]
    InvalidFinancialField {
        rowid: i64,
        aggregate_type: &'static str,
        event_type: String,
        field: &'static str,
        value: String,
        parse_error: String,
    },
    #[error("invalid symbol filter: {value}")]
    InvalidSymbolFilter { value: String },
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
            .map(|value| {
                et_day_start(value, "fromDate").and_then(|start| {
                    start
                        .checked_sub_signed(Duration::days(ALPACA_ACTIVITY_FETCH_PADDING_DAYS))
                        .ok_or_else(|| PnlError::InvalidDate {
                            field: "fromDate",
                            value: value.to_owned(),
                        })
                })
            })
            .transpose()
    }

    pub(crate) fn activity_until(&self) -> Result<Option<DateTime<Utc>>, PnlError> {
        self.to_date
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| {
                let date = parse_query_date(value, "toDate")?;
                let next_day =
                    date.checked_add_days(Days::new(1))
                        .ok_or_else(|| PnlError::InvalidDate {
                            field: "toDate",
                            value: value.to_owned(),
                        })?;
                et_midnight(next_day, "toDate", value).and_then(|end| {
                    end.checked_add_signed(Duration::days(ALPACA_ACTIVITY_FETCH_PADDING_DAYS))
                        .ok_or_else(|| PnlError::InvalidDate {
                            field: "toDate",
                            value: value.to_owned(),
                        })
                })
            })
            .transpose()
    }

    pub(crate) fn symbol_filter(
        &self,
        warnings: &mut Vec<String>,
    ) -> Result<BTreeSet<String>, PnlError> {
        let Some(raw) = &self.symbol else {
            return Ok(BTreeSet::new());
        };

        let mut symbols = BTreeSet::new();
        let mut invalid = Vec::new();
        let mut saw_filter_value = false;
        for symbol in raw
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            saw_filter_value = true;
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

        if saw_filter_value && symbols.is_empty() {
            return Err(PnlError::InvalidSymbolFilter { value: raw.clone() });
        }

        Ok(symbols)
    }
}

fn parse_query_date(value: &str, field: &'static str) -> Result<NaiveDate, PnlError> {
    if value.len() != 10 {
        return Err(PnlError::InvalidDate {
            field,
            value: value.to_owned(),
        });
    }

    NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| PnlError::InvalidDate {
        field,
        value: value.to_owned(),
    })
}

fn et_day_start(value: &str, field: &'static str) -> Result<DateTime<Utc>, PnlError> {
    let date = parse_query_date(value, field)?;
    et_midnight(date, field, value)
}

fn et_midnight(
    date: NaiveDate,
    field: &'static str,
    source_value: &str,
) -> Result<DateTime<Utc>, PnlError> {
    New_York
        .with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
        .single()
        .map(|datetime| datetime.with_timezone(&Utc))
        .ok_or_else(|| PnlError::InvalidDate {
            field,
            value: source_value.to_owned(),
        })
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
