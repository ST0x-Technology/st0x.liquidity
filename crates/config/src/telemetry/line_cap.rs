//! Keeps every console JSON line under Docker's 16 KiB split.
//!
//! Docker's json-file driver splits a line longer than 16384 bytes into
//! partial records of 16384 bytes. The Ops Agent does not join them again, so
//! each piece of a long line reaches Cloud Logging as unparsed text in
//! `jsonPayload.message`, and no part of it parses as the original line.
//! [`CappedLine`] makes every console line one complete JSON object of at most
//! [`MAX_CONSOLE_LINE_BYTES`], whatever the level or target.

use itertools::Itertools;
use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
use serde_json::Value;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};

/// The longest console line, newline included. Docker splits at 16384 bytes
/// of line content; the margin leaves room for anything the logging path adds
/// to the line before the split.
pub(super) const MAX_CONSOLE_LINE_BYTES: usize = 16_000;

/// Top-level keys that the cap never changes. `message` is the exception of
/// last resort: it is cut only when the line is still too long after every
/// other string was cut.
const KEPT_KEYS: [&str; 4] = ["timestamp", "level", "target", "message"];

/// Strings this short are not cut: the marker would be about as long as what
/// it removes.
const MIN_CUT_STRING_BYTES: usize = 64;

/// The key a line that cannot fit even with every string cut gets in place of
/// its other fields: the byte length of the original line.
const ORIGINAL_BYTES_KEY: &str = "line_original_bytes";

/// Formats the event with `F`, then writes the line unchanged when it fits in
/// [`MAX_CONSOLE_LINE_BYTES`] and capped by [`cap_line`] when it does not.
pub(super) struct CappedLine<F>(pub(super) F);

impl<S, N, F> FormatEvent<S, N> for CappedLine<F>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    F: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let mut line = String::new();
        self.0.format_event(ctx, Writer::new(&mut line), event)?;

        if line.len() <= MAX_CONSOLE_LINE_BYTES {
            return writer.write_str(&line);
        }

        let capped = cap_line(&line, MAX_CONSOLE_LINE_BYTES)
            .unwrap_or_else(|| dropped_line(event.metadata(), line.len()));
        writer.write_str(&capped)
    }
}

/// Rewrites a JSON `line` that is longer than `max_bytes` (newline included)
/// so it fits. It cuts the longest string values first, anywhere in the line
/// but in the [`KEPT_KEYS`], and ends each cut value with
/// `...[truncated N bytes]`. If the line is still too long, it cuts `message`.
/// If even that is not enough, it keeps only the [`KEPT_KEYS`] and
/// [`ORIGINAL_BYTES_KEY`], and cuts `message` only as far as that smaller line
/// needs. Every cut is on a character boundary, so the line stays valid UTF-8.
/// Top-level key order stays as it was.
///
/// Returns `None` when `line` is not one JSON object, or when it still does
/// not fit (only possible if `timestamp`, `level` or `target` is too long).
fn cap_line(line: &str, max_bytes: usize) -> Option<String> {
    let budget = max_bytes.saturating_sub(1);
    let body = line.strip_suffix('\n').unwrap_or(line);
    let OrderedObject(mut fields) = serde_json::from_str(body).ok()?;

    if !cut_until_fits(&mut fields, budget, |key| !KEPT_KEYS.contains(&key)) {
        let message_uncut = fields.clone();
        if !cut_until_fits(&mut fields, budget, |key| key == "message") {
            fields = message_uncut;
            fields.retain(|(key, _)| KEPT_KEYS.contains(&key.as_str()));
            fields.push((ORIGINAL_BYTES_KEY.to_string(), Value::from(line.len())));
            cut_until_fits(&mut fields, budget, |key| key == "message");
        }
    }

    let capped = format!("{}\n", serialize(&fields));
    (capped.len() <= max_bytes).then_some(capped)
}

/// Cuts the longest string under a top-level key that `may_cut` accepts,
/// repeatedly, until the line is at most `budget` bytes. Returns whether it
/// fits.
fn cut_until_fits(
    fields: &mut [(String, Value)],
    budget: usize,
    may_cut: impl Fn(&str) -> bool,
) -> bool {
    loop {
        let len = serialized_len(fields);
        if len <= budget {
            return true;
        }

        let mut strings = Vec::new();
        for (key, value) in fields.iter_mut() {
            if may_cut(key) {
                string_values(value, &mut strings);
            }
        }

        let Some(longest) = strings
            .into_iter()
            .filter(|string| !is_cut(string) && string.len() >= MIN_CUT_STRING_BYTES)
            .max_by_key(|string| string.len())
        else {
            return false;
        };

        cut_string(longest, len - budget);
    }
}

/// Collects every string in `value`, nested ones included.
fn string_values<'a>(value: &'a mut Value, strings: &mut Vec<&'a mut String>) {
    match value {
        Value::String(string) => strings.push(string),
        Value::Array(items) => {
            for item in items {
                string_values(item, strings);
            }
        }
        Value::Object(object) => {
            for item in object.values_mut() {
                string_values(item, strings);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

const MARKER_START: &str = "...[truncated ";
const MARKER_END: &str = " bytes]";

fn marker(removed_bytes: usize) -> String {
    format!("{MARKER_START}{removed_bytes}{MARKER_END}")
}

/// Whether [`cut_string`] already cut `string`, so it is not cut twice.
fn is_cut(string: &str) -> bool {
    string
        .strip_suffix(MARKER_END)
        .and_then(|rest| rest.rsplit_once(MARKER_START))
        .is_some_and(|(_, count)| {
            !count.is_empty() && count.bytes().all(|byte| byte.is_ascii_digit())
        })
}

/// Shortens `string` so its JSON form is at least `excess` bytes shorter, or
/// as short as it can be, and appends the marker. The cut lands on a
/// character boundary.
fn cut_string(string: &mut String, excess: usize) {
    let escaped_len: usize = string.chars().map(escaped_char_len).sum();
    let longest_marker = marker(string.len())
        .chars()
        .map(escaped_char_len)
        .sum::<usize>();
    let keep_escaped = escaped_len.saturating_sub(excess + longest_marker);

    let mut kept_escaped = 0;
    let cut_at = string
        .char_indices()
        .find_map(|(index, character)| {
            kept_escaped += escaped_char_len(character);
            (kept_escaped > keep_escaped).then_some(index)
        })
        .unwrap_or(string.len());

    let removed = string.len() - cut_at;
    string.truncate(cut_at);
    string.push_str(&marker(removed));
}

/// The bytes `character` takes inside a JSON string as `serde_json` writes
/// it: a two-byte escape for `"`, `\` and the short control escapes, `\u00XX`
/// for the other control characters, and its UTF-8 bytes otherwise.
fn escaped_char_len(character: char) -> usize {
    match character {
        '"' | '\\' | '\u{8}' | '\u{c}' | '\n' | '\r' | '\t' => 2,
        '\u{0}'..='\u{1f}' => 6,
        _ => character.len_utf8(),
    }
}

fn serialize(fields: &[(String, Value)]) -> String {
    let body = fields
        .iter()
        .map(|(key, value)| format!("{}:{value}", Value::from(key.as_str())))
        .join(",");
    format!("{{{body}}}")
}

fn serialized_len(fields: &[(String, Value)]) -> usize {
    serialize(fields).len()
}

/// The line written in place of an over-long line that [`cap_line`] cannot
/// cut to fit. It has a new timestamp, and its keys are in sorted order. Not
/// expected: the wrapped formatter writes JSON, and a dependency would need a
/// field named `timestamp`, `level` or `target` that is too long.
fn dropped_line(metadata: &tracing::Metadata<'_>, original_bytes: usize) -> String {
    let line = serde_json::json!({
        "timestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        "level": metadata.level().as_str(),
        "target": metadata.target(),
        "message": "Console log line was too long and could not be cut to fit, so it was dropped",
        ORIGINAL_BYTES_KEY: original_bytes,
    });
    format!("{line}\n")
}

/// A JSON object read with its keys in line order. `serde_json::Map` sorts
/// them, which would move `timestamp`, `level` and `target` on a capped line.
struct OrderedObject(Vec<(String, Value)>);

impl<'de> Deserialize<'de> for OrderedObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct OrderedVisitor;

        impl<'de> Visitor<'de> for OrderedVisitor {
            type Value = OrderedObject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<OrderedObject, A::Error> {
                let mut fields = Vec::new();
                while let Some(entry) = map.next_entry::<String, Value>()? {
                    fields.push(entry);
                }
                Ok(OrderedObject(fields))
            }
        }

        deserializer.deserialize_map(OrderedVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line_with(fields: &[(&str, Value)]) -> String {
        let fields = fields
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect_vec();
        format!("{}\n", serialize(&fields))
    }

    fn kept_fields() -> Vec<(&'static str, Value)> {
        vec![
            ("timestamp", Value::from("2026-10-16T13:00:00.000000Z")),
            ("level", Value::from("TRACE")),
            ("target", Value::from("st0x_alpaca")),
            ("message", Value::from("Alpaca Broker API response")),
        ]
    }

    fn parse(line: &str) -> serde_json::Value {
        assert!(line.ends_with('\n'), "{line}");
        assert_eq!(line.matches('\n').count(), 1, "one line: {line}");
        serde_json::from_str(line.trim_end()).unwrap()
    }

    #[test]
    fn a_line_with_a_large_field_fits_and_keeps_the_kept_keys() {
        let mut fields = kept_fields();
        fields.push(("body", Value::from("a".repeat(100_000))));
        fields.push(("status", Value::from(200)));
        let line = line_with(&fields);

        let capped = cap_line(&line, MAX_CONSOLE_LINE_BYTES).unwrap();

        assert!(capped.len() <= MAX_CONSOLE_LINE_BYTES, "{}", capped.len());
        let entry = parse(&capped);
        for (key, value) in kept_fields() {
            assert_eq!(entry[key], value, "{key}");
        }
        assert_eq!(entry["status"], 200);

        let body = entry["body"].as_str().unwrap();
        let (kept, marker) = body.split_once("...[").unwrap();
        assert!(kept.bytes().all(|byte| byte == b'a'));
        assert_eq!(marker, format!("truncated {} bytes]", 100_000 - kept.len()));
        assert!(
            capped.len() > MAX_CONSOLE_LINE_BYTES - 64,
            "the cut keeps as much of the field as fits: {}",
            capped.len()
        );

        let keys = parse_keys(&capped);
        assert_eq!(
            keys,
            ["timestamp", "level", "target", "message", "body", "status"]
        );
    }

    fn parse_keys(line: &str) -> Vec<String> {
        let OrderedObject(fields) = serde_json::from_str(line.trim_end()).unwrap();
        fields.into_iter().map(|(key, _)| key).collect()
    }

    #[test]
    fn a_cut_never_splits_a_multi_byte_character() {
        for character in ['é', '€', '🦀'] {
            for offset in 0..4 {
                let mut fields = kept_fields();
                let value = format!(
                    "{}{}",
                    "a".repeat(offset),
                    character.to_string().repeat(20_000)
                );
                fields.push(("body", Value::from(value.clone())));
                let line = line_with(&fields);

                let capped = cap_line(&line, MAX_CONSOLE_LINE_BYTES).unwrap();

                assert!(capped.len() <= MAX_CONSOLE_LINE_BYTES);
                let entry = parse(&capped);
                let body = entry["body"].as_str().unwrap();
                let kept = &body[..body.rfind(MARKER_START).unwrap()];
                assert!(value.starts_with(kept), "{character} at offset {offset}");
                assert!(kept.ends_with(character), "{character} at offset {offset}");
            }
        }
    }

    #[test]
    fn escaped_characters_count_at_their_escaped_length() {
        let mut fields = kept_fields();
        fields.push(("body", Value::from("\"\\\n\u{1}".repeat(10_000))));
        let line = line_with(&fields);

        let capped = cap_line(&line, MAX_CONSOLE_LINE_BYTES).unwrap();

        assert!(capped.len() <= MAX_CONSOLE_LINE_BYTES, "{}", capped.len());
        assert!(
            capped.len() > MAX_CONSOLE_LINE_BYTES - 64,
            "{}",
            capped.len()
        );
        parse(&capped);
    }

    #[test]
    fn the_longest_fields_are_cut_first_and_short_ones_stay() {
        let mut fields = kept_fields();
        fields.push(("short", Value::from("keep me")));
        fields.push(("medium", Value::from("m".repeat(8_000))));
        fields.push(("large", Value::from("l".repeat(30_000))));
        let line = line_with(&fields);

        let capped = cap_line(&line, MAX_CONSOLE_LINE_BYTES).unwrap();

        let entry = parse(&capped);
        assert_eq!(entry["short"], "keep me");
        assert_eq!(entry["medium"], "m".repeat(8_000));
        assert!(is_cut(entry["large"].as_str().unwrap()));
    }

    #[test]
    fn many_large_fields_are_each_cut_once() {
        let mut fields = kept_fields();
        let names = ["one", "two", "three", "four"];
        for name in names {
            fields.push((name, Value::from("x".repeat(10_000))));
        }
        let line = line_with(&fields);

        let capped = cap_line(&line, MAX_CONSOLE_LINE_BYTES).unwrap();

        assert!(capped.len() <= MAX_CONSOLE_LINE_BYTES);
        let entry = parse(&capped);
        for name in names {
            let value = entry[name].as_str().unwrap();
            assert!(value.matches(MARKER_START).count() <= 1, "{name}: {value}");
        }
        assert_eq!(entry["message"], "Alpaca Broker API response");
    }

    #[test]
    fn strings_inside_span_context_are_cut_too() {
        let mut fields = kept_fields();
        fields.push((
            "span",
            serde_json::json!({"name": "poll", "request": "r".repeat(40_000)}),
        ));
        let line = line_with(&fields);

        let capped = cap_line(&line, MAX_CONSOLE_LINE_BYTES).unwrap();

        assert!(capped.len() <= MAX_CONSOLE_LINE_BYTES);
        let entry = parse(&capped);
        assert_eq!(entry["span"]["name"], "poll");
        assert!(is_cut(entry["span"]["request"].as_str().unwrap()));
    }

    #[test]
    fn a_message_is_cut_only_when_nothing_else_is_left_to_cut() {
        let mut fields = kept_fields();
        fields[3].1 = Value::from("m".repeat(40_000));
        fields.push(("body", Value::from("b".repeat(1_000))));
        let line = line_with(&fields);

        let capped = cap_line(&line, MAX_CONSOLE_LINE_BYTES).unwrap();

        assert!(capped.len() <= MAX_CONSOLE_LINE_BYTES);
        let entry = parse(&capped);
        assert!(is_cut(entry["body"].as_str().unwrap()));
        assert!(is_cut(entry["message"].as_str().unwrap()));
        assert_eq!(entry["level"], "TRACE");
        assert_eq!(entry["target"], "st0x_alpaca");
    }

    #[test]
    fn a_line_too_long_without_strings_keeps_only_the_kept_keys() {
        let mut fields = kept_fields();
        fields.push(("numbers", Value::from((0..10_000).collect_vec())));
        let line = line_with(&fields);

        let capped = cap_line(&line, MAX_CONSOLE_LINE_BYTES).unwrap();

        assert!(capped.len() <= MAX_CONSOLE_LINE_BYTES);
        let entry = parse(&capped);
        let mut expected: serde_json::Map<String, Value> = kept_fields()
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect();
        expected.insert(ORIGINAL_BYTES_KEY.to_string(), Value::from(line.len()));
        assert_eq!(entry, Value::Object(expected));
    }

    #[test]
    fn a_message_that_fits_once_the_other_fields_are_dropped_stays_whole() {
        let mut fields = kept_fields();
        fields[3].1 = Value::from("m".repeat(10_000));
        fields.push(("numbers", Value::from((0..10_000).collect_vec())));
        let line = line_with(&fields);

        let capped = cap_line(&line, MAX_CONSOLE_LINE_BYTES).unwrap();

        assert!(capped.len() <= MAX_CONSOLE_LINE_BYTES);
        let entry = parse(&capped);
        assert_eq!(entry["message"], "m".repeat(10_000));
        assert_eq!(entry[ORIGINAL_BYTES_KEY], line.len());
        assert!(entry.get("numbers").is_none());
    }

    #[test]
    fn a_line_whose_kept_keys_alone_are_too_long_is_left_to_the_caller() {
        let mut fields = kept_fields();
        fields.push(("target", Value::from("t".repeat(20_000))));
        let line = line_with(&fields);

        assert_eq!(cap_line(&line, MAX_CONSOLE_LINE_BYTES), None);
    }

    #[test]
    fn a_line_that_is_not_a_json_object_is_left_to_the_caller() {
        assert_eq!(cap_line(&"x".repeat(20_000), MAX_CONSOLE_LINE_BYTES), None);
    }
}
