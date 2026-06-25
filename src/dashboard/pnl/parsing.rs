use std::str::FromStr;

use chrono::{DateTime, NaiveDate, Utc};
use num_decimal::Num;
use serde_json::Value;

use super::SAFE_SYMBOL_CHARS;
use super::query::PnlError;
use super::state::{Direction, PositionEventRow};

pub(crate) fn parse_payload_string(payload: &str) -> Value {
    serde_json::from_str(payload).unwrap_or(Value::Null)
}

pub(crate) fn parse_query_datetime(value: &str) -> Result<DateTime<Utc>, PnlError> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        return Ok(parsed.with_timezone(&Utc));
    }

    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| PnlError::InvalidQuery(format!("invalid timestamp/date: {value}")))?;
    let datetime = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| PnlError::InvalidQuery(format!("invalid timestamp/date: {value}")))?;
    Ok(DateTime::from_naive_utc_and_offset(datetime, Utc))
}

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

pub(crate) fn nested_record<'a>(payload: &'a Value, key: &str) -> Option<&'a Value> {
    payload.get(key).filter(|value| value.is_object())
}

pub(crate) fn text_field(payload: &Value, key: &str) -> Option<String> {
    payload.get(key).and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        _ => None,
    })
}

pub(crate) fn number_text_field(payload: &Value, key: &str) -> Option<String> {
    payload.get(key).and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub(crate) fn decimal_field(payload: &Value, key: &str) -> Option<Num> {
    number_text_field(payload, key).and_then(|value| Num::from_str(&value).ok())
}

pub(crate) fn direction_field(payload: &Value, key: &str) -> Option<Direction> {
    match text_field(payload, key)?.as_str() {
        "Buy" | "buy" => Some(Direction::Buy),
        "Sell" | "sell" => Some(Direction::Sell),
        _ => None,
    }
}

pub(crate) fn position_event_replay_timestamp(row: &PositionEventRow) -> Option<String> {
    match row.event_type.as_str() {
        "PositionEvent::OnChainOrderFilled" => nested_record(&row.payload, "OnChainOrderFilled")
            .and_then(|filled| text_field(filled, "block_timestamp")),
        "PositionEvent::OffChainOrderFilled" => nested_record(&row.payload, "OffChainOrderFilled")
            .and_then(|filled| text_field(filled, "broker_timestamp")),
        "PositionEvent::OffChainOrderPlaced" => nested_record(&row.payload, "OffChainOrderPlaced")
            .and_then(|placed| text_field(placed, "placed_at")),
        _ => None,
    }
}

pub(crate) fn ordered_position_events(
    rows: Vec<PositionEventRow>,
    warnings: &mut Vec<String>,
) -> Vec<PositionEventRow> {
    let mut sortable: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let timestamp = position_event_replay_timestamp(&row);
            let timestamp_ms = timestamp
                .as_deref()
                .and_then(parse_timestamp)
                .map(|parsed| parsed.timestamp_millis())
                .unwrap_or(i64::MAX);
            if timestamp.is_some() && timestamp_ms == i64::MAX {
                warnings.push(format!(
                    "PnL audit note: invalid replay timestamp for {} row {}; using event row order",
                    row.symbol, row.rowid
                ));
            }

            (timestamp_ms, row.rowid, row)
        })
        .collect();

    sortable.sort_by_key(|(timestamp_ms, rowid, _)| (*timestamp_ms, *rowid));
    sortable.into_iter().map(|(_, _, row)| row).collect()
}

pub(crate) fn fmt_decimal(value: &Num) -> String {
    let rounded = format!("{value:.9}");
    let trimmed = rounded.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() || trimmed == "-0" {
        "0".to_owned()
    } else {
        trimmed.to_owned()
    }
}

pub(crate) fn abs_decimal(value: &Num) -> Num {
    if value.is_negative() {
        -value.clone()
    } else {
        value.clone()
    }
}

pub(crate) fn min_decimal(left: &Num, right: &Num) -> Num {
    if left <= right {
        left.clone()
    } else {
        right.clone()
    }
}

pub(crate) fn parse_decimal_lossy(value: &str) -> Num {
    Num::from_str(value).unwrap_or_default()
}
