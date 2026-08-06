//! Backend PnL query model, validation, and date-range normalization.
use chrono::{DateTime, Datelike, Days, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use serde::Deserialize;
use std::collections::BTreeSet;

use super::parsing::is_safe_symbol;
use crate::portfolio_snapshot::EtDayRange;

const ALPACA_ACTIVITY_FETCH_PADDING_DAYS: i64 = 7;

#[derive(Debug, thiserror::Error)]
pub(crate) enum PnlFinancialFieldError {
    // Boxed: `FloatError` embeds revm's `EVMError`/`HaltReason`, which makes
    // it far larger than every other payload here, and this error is returned
    // from most of the module's functions.
    #[error("invalid decimal: {0}")]
    InvalidDecimal(#[source] Box<rain_math_float::FloatError>),
}

/// Sentinel `event_type` for errors raised from typed ledger rows, which --
/// unlike the raw payloads the pre-ADR-0018 path parsed -- carry no
/// per-event type string of their own.
pub(crate) const LEDGER_ROW_EVENT_TYPE: &str = "ledger_row";

#[derive(Debug, thiserror::Error)]
pub(crate) enum PnlError {
    #[error("invalid {field}: {value}")]
    InvalidDate { field: &'static str, value: String },
    #[error("invalid asOfRowid: {value}")]
    InvalidSnapshotRowid { value: i64 },
    #[error("PnL ledger ingestion failed: {0}")]
    Ledger(#[from] super::ledger::PnlLedgerError),
    #[error("invalid PnL ledger row in {table} at event row {rowid}: {reason}")]
    InvalidLedgerRow {
        table: &'static str,
        rowid: i64,
        reason: &'static str,
    },
    #[error(
        "malformed persisted PnL payload at row {rowid} ({aggregate_type}/{event_type}): {reason}"
    )]
    MalformedPayload {
        rowid: i64,
        aggregate_type: &'static str,
        event_type: String,
        reason: &'static str,
    },
    #[error(
        "failed to parse persisted financial field {field} at row {rowid} \
         ({aggregate_type}/{event_type}): {value} ({source})"
    )]
    InvalidFinancialField {
        rowid: i64,
        aggregate_type: &'static str,
        event_type: String,
        field: &'static str,
        value: String,
        #[source]
        source: PnlFinancialFieldError,
    },
    #[error("failed to parse internal PnL decimal field {field}: {value}")]
    InvalidInternalDecimal {
        field: &'static str,
        value: String,
        #[source]
        source: Box<rain_math_float::FloatError>,
    },
    #[error("invalid symbol filter: {value}")]
    InvalidSymbolFilter { value: String },
    #[error("failed to load PnL rows: {0}")]
    Database(#[from] sqlx::Error),
    #[error("failed to load portfolio snapshot data for capital/return computation: {0}")]
    PortfolioSnapshot(#[from] crate::portfolio_snapshot::ReadError),
    #[error("PnL arithmetic failed: {0}")]
    Arithmetic(#[source] Box<ArithmeticFailure>),
    #[error("PnL report admission capacity exhausted: {0}")]
    ReplayAdmission(#[from] tokio::sync::TryAcquireError),
    #[error("PnL replay worker failed: {0}")]
    ReplayWorker(#[from] tokio::task::JoinError),
}

/// A `Float` arithmetic/comparison/formatting failure, tagged with the source
/// location of the `?` that converted it. Nearly every function in this
/// module is fallible on `Float` now, so the location -- not a hand-picked
/// "operation" label -- is what actually distinguishes one failure site from
/// another (cost summary vs. FIFO replay vs. capital block vs. window
/// aggregation) when `/pnl` logs a 500. `#[track_caller]` on the `From` impl
/// below makes `Location::caller()` resolve to the exact `?` call site, not
/// this impl's own body. A handful of arithmetic accumulator helpers
/// (`replay.rs`'s `add_venue_notional`/`add_realized_pnl`/`add_summary`) are
/// invoked from more than one pipeline stage; those are themselves marked
/// `#[track_caller]` so the location forwards one frame further, to the
/// stage that called the helper, rather than to a line inside the shared
/// helper that is identical regardless of which stage triggered it.
#[derive(Debug, thiserror::Error)]
#[error("{location}: {source}")]
pub(crate) struct ArithmeticFailure {
    pub(super) location: &'static std::panic::Location<'static>,
    #[source]
    source: Box<rain_math_float::FloatError>,
}

impl From<rain_math_float::FloatError> for PnlError {
    #[track_caller]
    fn from(error: rain_math_float::FloatError) -> Self {
        Self::Arithmetic(Box::new(ArithmeticFailure {
            location: std::panic::Location::caller(),
            source: Box::new(error),
        }))
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PnlQuery {
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
    pub(crate) as_of_rowid: Option<i64>,
    pub(crate) symbol: Option<String>,
    pub(crate) from_date: Option<String>,
    pub(crate) to_date: Option<String>,
    pub(crate) market_session_filter: Option<PnlMarketSessionFilter>,
    pub(crate) counter_trading_filter: Option<PnlCounterTradingFilter>,
}

impl PnlQuery {
    pub(crate) fn normalized_limit(&self) -> usize {
        self.limit.unwrap_or(100).clamp(1, 5_000)
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

    /// The query's `fromDate`/`toDate` bounds as independent, optionally-open
    /// ET-day bounds (inclusive), reusing [`parse_query_date`] -- no new
    /// query params. Each side is `None` when that bound is not set; no
    /// sentinel dates stand in for "unbounded". Using `0001-01-01` or
    /// `9999-12-31` to widen a missing side would make the sentinel
    /// indistinguishable from the same literal date supplied by a client.
    /// Downstream query building (`load_portfolio_days`) branches on each
    /// side's presence directly instead.
    pub(crate) fn et_day_range(&self) -> Result<EtDayRange, PnlError> {
        let from = self
            .from_date
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| parse_query_date(value, "fromDate"))
            .transpose()?;
        let to = self
            .to_date
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| parse_query_date(value, "toDate"))
            .transpose()?;

        Ok(EtDayRange { from, to })
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
