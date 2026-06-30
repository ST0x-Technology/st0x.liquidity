use std::str::FromStr;

use chrono::{DateTime, Utc};
use num_decimal::Num;
use num_decimal::num_bigint::BigInt;
use num_traits::Zero;
use serde_json::Value;

use super::SAFE_SYMBOL_CHARS;
use super::query::PnlError;
use super::state::{Direction, PositionEventRow};

pub(crate) fn parse_payload_string(payload: &str) -> Result<Value, serde_json::Error> {
    serde_json::from_str(payload)
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

pub(crate) fn persisted_decimal_field(
    row: &PositionEventRow,
    payload: &Value,
    key: &'static str,
) -> Result<Option<Num>, PnlError> {
    persisted_decimal_value(row.rowid, "Position", row.event_type.clone(), payload, key)
}

pub(crate) fn persisted_decimal_value(
    rowid: i64,
    aggregate_type: &'static str,
    event_type: String,
    payload: &Value,
    key: &'static str,
) -> Result<Option<Num>, PnlError> {
    let value = match payload.get(key) {
        None => return Ok(None),
        Some(Value::String(text)) => text.clone(),
        Some(Value::Number(number)) => number.to_string(),
        Some(value) => {
            return Err(PnlError::InvalidFinancialField {
                rowid,
                aggregate_type,
                event_type,
                field: key,
                value: value.to_string(),
                parse_error: "expected string or number".to_owned(),
            });
        }
    };

    Num::from_str(&value)
        .map(Some)
        .map_err(|error| PnlError::InvalidFinancialField {
            rowid,
            aggregate_type,
            event_type,
            field: key,
            value,
            parse_error: error.to_string(),
        })
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
        "PositionEvent::ManualPositionAdjusted" => {
            nested_record(&row.payload, "ManualPositionAdjusted")
                .and_then(|adjusted| text_field(adjusted, "adjusted_at"))
        }
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
                .map_or(i64::MAX, |parsed| parsed.timestamp_millis());
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
    let (mut numerator, denominator): (BigInt, BigInt) = value.clone().into();
    if numerator.is_zero() {
        return "0".to_owned();
    }

    let negative = numerator.sign() == num_decimal::num_bigint::Sign::Minus;
    if negative {
        numerator = -numerator;
    }

    let mut denominator = denominator;
    let twos = factor_count(&mut denominator, 2);
    let fives = factor_count(&mut denominator, 5);
    assert!(
        denominator == BigInt::from(1),
        "PnL decimal output must be a finite decimal: {value:?}"
    );

    let scale = twos.max(fives);
    let scaled = multiply_factor(
        multiply_factor(numerator, 2, scale - twos),
        5,
        scale - fives,
    );
    let mut digits = scaled.to_string();

    if scale > 0 {
        if digits.len() <= scale {
            digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
        }
        let dot_index = digits.len() - scale;
        digits.insert(dot_index, '.');
        while digits.ends_with('0') {
            digits.pop();
        }
        if digits.ends_with('.') {
            digits.pop();
        }
    }

    if negative {
        format!("-{digits}")
    } else {
        digits
    }
}

fn factor_count(value: &mut BigInt, factor: u8) -> usize {
    let factor = BigInt::from(factor);
    let mut count = 0;
    while (&*value % &factor).is_zero() {
        *value /= &factor;
        count += 1;
    }
    count
}

fn multiply_factor(mut value: BigInt, factor: u8, count: usize) -> BigInt {
    let factor = BigInt::from(factor);
    for _ in 0..count {
        value *= &factor;
    }
    value
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

pub(crate) fn parse_internal_decimal(value: &str) -> Num {
    Num::from_str(value)
        .unwrap_or_else(|error| panic!("internal PnL decimal field must parse: {value}: {error}"))
}
