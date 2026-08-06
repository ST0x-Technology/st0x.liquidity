//! Parsing helpers for ledger-row decimals, timestamps, and report output.
use chrono::{DateTime, Utc};
use rain_math_float::Float;

use super::SAFE_SYMBOL_CHARS;
use super::query::{LEDGER_ROW_EVENT_TYPE, PnlError, PnlFinancialFieldError};
use super::state::PositionLedgerRow;

pub(crate) fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|parsed| parsed.with_timezone(&Utc))
        .ok()
}

pub(crate) fn is_safe_symbol(symbol: &str) -> bool {
    !symbol.is_empty()
        && symbol
            .chars()
            .all(|character| SAFE_SYMBOL_CHARS.contains(character))
}

/// Parses a canonical decimal string stored in a ledger column. Failure
/// means the ledger row is corrupt (the ingester only writes `format_float`
/// output), so the report fails closed with the row's provenance.
pub(crate) fn parse_ledger_decimal(
    table: &'static str,
    rowid: i64,
    field: &'static str,
    value: &str,
) -> Result<Float, PnlError> {
    Float::parse(value.to_owned()).map_err(|error| PnlError::InvalidFinancialField {
        rowid,
        aggregate_type: table,
        event_type: LEDGER_ROW_EVENT_TYPE.to_owned(),
        field,
        value: value.to_owned(),
        source: PnlFinancialFieldError::InvalidDecimal(Box::new(error)),
    })
}

/// Sorts the ledger rows into replay order: execution timestamp first, event
/// rowid as the deterministic tie-breaker. Timestamps are always present on
/// ledger rows (typed at ingestion); an unparseable one means a corrupt row
/// and fails the report with its provenance.
pub(crate) fn ordered_position_events(
    rows: Vec<PositionLedgerRow>,
) -> Result<Vec<PositionLedgerRow>, PnlError> {
    let mut sortable: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let timestamp_ms = parse_timestamp(row.replay_timestamp())
                .map(|parsed| parsed.timestamp_millis())
                .ok_or_else(|| PnlError::InvalidLedgerRow {
                    table: "pnl position rows",
                    rowid: row.event_rowid(),
                    reason: "invalid replay timestamp",
                })?;

            Ok((timestamp_ms, row.event_rowid(), row))
        })
        .collect::<Result<Vec<_>, PnlError>>()?;

    sortable.sort_by_key(|(timestamp_ms, rowid, _)| (*timestamp_ms, *rowid));
    Ok(sortable.into_iter().map(|(_, _, row)| row).collect())
}

/// Formats a report value as a decimal string.
///
/// `Float` carries a 224-bit coefficient, wide enough for any report value.
/// Delegates to the shared formatter rather than calling
/// `format_with_scientific(false)` directly: that rejects exponents below
/// -76, which a small residual PnL value reaches, and the shared helper
/// falls back to scientific notation instead of losing the value.
pub(crate) fn fmt_decimal(value: Float) -> Result<String, PnlError> {
    Ok(st0x_float_serde::format_float(&value)?)
}

pub(crate) fn parse_internal_decimal(field: &'static str, value: &str) -> Result<Float, PnlError> {
    Float::parse(value.to_owned()).map_err(|source| PnlError::InvalidInternalDecimal {
        field,
        value: value.to_owned(),
        source: Box::new(source),
    })
}
