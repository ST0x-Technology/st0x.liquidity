//! Order classification: extracts Rainlang source from order metadata
//! and pattern-matches it to determine the order's pricing mechanism.
//!
//! The Rainlang source is stored as CBOR-encoded metadata in `MetaV1_2`
//! events emitted alongside `AddOrderV3`. The metadata format is
//! `RainMetaDocumentV1`: an 8-byte magic prefix followed by one or more
//! CBOR maps, each containing a payload, magic number, and content type.
//!
//! Classification categories:
//! - **FixedPrice**: literal constants only (e.g., `_ _: 1000 185e18;:;`)
//! - **PythOracle**: contains Pyth price feed reads
//! - **Unsupported**: anything else (storage reads, time-dependent, etc.)

use alloy::primitives::Bytes;
use tracing::{debug, warn};

use crate::tracked_order::OrderType;

/// Magic prefix for RainMetaDocumentV1 format.
pub(crate) const RAIN_META_DOCUMENT_V1_MAGIC: [u8; 8] =
    [0xff, 0x0a, 0x89, 0xc6, 0x74, 0xee, 0x78, 0x74];

/// Classifies an order by extracting Rainlang source from metadata
/// and pattern-matching the expression.
///
/// Returns `OrderType::Unknown` if metadata is empty or cannot be
/// decoded (orders without metadata are treated as unclassified,
/// not unsupported — they may be classifiable via other means later).
pub(crate) fn classify_order_metadata(meta: &Bytes) -> OrderType {
    if meta.is_empty() {
        return OrderType::Unknown;
    }

    let rainlang = match extract_rainlang_source(meta) {
        Ok(source) => source,
        Err(error) => {
            warn!("Failed to extract Rainlang from metadata: {error}");
            return OrderType::Unknown;
        }
    };

    classify_rainlang(&rainlang)
}

/// Magic number identifying a CBOR item as Rainlang source (key 1 in the map).
/// See `KnownMagic::RainlangSourceV1` in rain.metadata.
pub(crate) const RAINLANG_SOURCE_V1_MAGIC: u64 = 0xff13_109e_4133_6ff2;

/// Extracts the Rainlang source string from raw metadata bytes.
///
/// Format: 8-byte magic prefix + concatenated CBOR maps.
/// Each map has keys: 0 (payload bytes), 1 (magic u64), 2 (content type).
/// Iterates all CBOR items and selects the one whose key 1 matches
/// `RainlangSourceV1` magic, verifying the item actually claims to be
/// Rainlang rather than blindly trusting the first payload.
fn extract_rainlang_source(meta: &[u8]) -> Result<String, ClassificationError> {
    if meta.len() < RAIN_META_DOCUMENT_V1_MAGIC.len() {
        return Err(ClassificationError::TooShort);
    }

    if meta[..8] != RAIN_META_DOCUMENT_V1_MAGIC {
        return Err(ClassificationError::InvalidMagicPrefix);
    }

    let cbor_data = &meta[8..];
    let mut cursor = std::io::Cursor::new(cbor_data);

    // Iterate all concatenated CBOR items, looking for the one with
    // RainlangSourceV1 magic (key 1). A RainMetaDocumentV1 may contain
    // multiple items (e.g., source + bytecode + deployer metadata).
    while (cursor.position()) < cbor_data.len() as u64 {
        let value: ciborium::Value = match ciborium::from_reader(&mut cursor) {
            Ok(val) => val,
            Err(_) => break,
        };

        let Some(map) = value.as_map() else {
            continue;
        };

        // Check key 1 (magic number) to verify this item is Rainlang source
        let is_rainlang = map.iter().any(|(key, val)| {
            key.as_integer() == Some(1.into())
                && val.as_integer() == Some(RAINLANG_SOURCE_V1_MAGIC.into())
        });

        if !is_rainlang {
            continue;
        }

        // Key 0 contains the payload (Rainlang source as bytes)
        let payload = map
            .iter()
            .find(|(key, _)| key.as_integer() == Some(0.into()))
            .and_then(|(_, val)| val.as_bytes())
            .ok_or(ClassificationError::MissingPayload)?;

        return String::from_utf8(payload.clone()).map_err(ClassificationError::Utf8);
    }

    Err(ClassificationError::MissingPayload)
}

/// Classifies a Rainlang source string by pattern matching.
fn classify_rainlang(rainlang: &str) -> OrderType {
    // Strip comments and normalize whitespace for pattern matching
    let stripped = strip_comments(rainlang);

    if is_pyth_oracle_expression(&stripped) {
        debug!("Classified as PythOracle");
        return OrderType::PythOracle {
            rainlang: rainlang.to_owned(),
        };
    }

    if is_fixed_price_expression(&stripped) {
        debug!("Classified as FixedPrice");
        return OrderType::FixedPrice {
            rainlang: rainlang.to_owned(),
        };
    }

    debug!("Classified as Unsupported");
    OrderType::Unsupported {
        reason: "unrecognized Rainlang pattern".to_owned(),
    }
}

/// Checks if the expression is a fixed-price order.
///
/// Fixed-price orders have only literal numeric values and `max-value()`
/// calls in their calculate-io source. No external calls, no storage
/// reads, no conditionals.
///
/// Patterns:
/// - `_ _: 1000 185e18;:;` (two literals, empty handle-io)
/// - `max-output: max-value(), io: 12;:;` (max-value + literal)
fn is_fixed_price_expression(stripped: &str) -> bool {
    // A fixed-price expression has only literals and max-value() calls.
    // After removing max-value(), no parentheses should remain (any
    // remaining `(` means a function call we don't recognize).
    let without_max_value = stripped.replace("max-value()", "MAXVAL");

    !without_max_value.contains('(')
}

/// Checks if the expression references a Pyth oracle.
///
/// Pyth oracle expressions contain calls to Pyth-specific words
/// like `pyth-price` or `pyth-v3`. We match on concrete Pyth word
/// patterns rather than generic terms like "oracle" to avoid
/// misclassifying non-Pyth oracle integrations.
fn is_pyth_oracle_expression(stripped: &str) -> bool {
    let lower = stripped.to_lowercase();

    // Match only concrete Pyth word names used in Rainlang
    let pyth_indicators = ["pyth-price", "pyth-v3", "pyth-feed"];

    pyth_indicators
        .iter()
        .any(|indicator| lower.contains(indicator))
}

/// Strips Rainlang comments (`/* ... */` block and `//` line comments).
fn strip_comments(source: &str) -> String {
    let mut result = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();

    while let Some(current) = chars.next() {
        if current == '/' {
            match chars.peek() {
                Some(&'*') => {
                    // Block comment: skip until */
                    chars.next(); // consume *
                    loop {
                        match chars.next() {
                            Some('*') if chars.peek() == Some(&'/') => {
                                chars.next(); // consume /
                                break;
                            }
                            None => break,
                            _ => {}
                        }
                    }
                    continue;
                }
                Some(&'/') => {
                    // Line comment: skip until end of line
                    chars.next(); // consume second /
                    for ch in chars.by_ref() {
                        if ch == '\n' {
                            result.push('\n');
                            break;
                        }
                    }
                    continue;
                }
                _ => {}
            }
        }
        result.push(current);
    }

    result
}

/// Errors from metadata extraction.
#[derive(Debug, thiserror::Error)]
enum ClassificationError {
    #[error("metadata too short (< 8 bytes)")]
    TooShort,

    #[error("invalid RainMetaDocumentV1 magic prefix")]
    InvalidMagicPrefix,

    #[error("no RainlangSourceV1 item found in metadata")]
    MissingPayload,

    #[error("payload is not valid UTF-8: {0}")]
    Utf8(std::string::FromUtf8Error),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Bytes;

    use super::*;

    /// Real metadata from Rain's test suite (see lib/rain.orderbook/crates/common/src/meta.rs).
    /// Contains the Rainlang source for a conditional order with max-value() and if/equal-to.
    const REAL_META_HEX: &str = "ff0a89c674ee7874a30058ef2f2a20302e2063616c63756c6174652d696f202a2f200a7573696e672d776f7264732d66726f6d203078466532343131434461313933443945346538334135633233344337466433323031303138383361430a6d61782d6f75747075743a206d61782d76616c756528292c0a696f3a206966280a2020657175616c2d746f280a202020206f75747075742d746f6b656e28290a202020203078316438306334396262626364316330393131333436363536623532396466396535633266373833640a2020290a202031320a2020696e76283132290a293b0a0a2f2a20312e2068616e646c652d696f202a2f200a3a3b011bff13109e41336ff20278186170706c69636174696f6e2f6f637465742d73747265616d";

    fn meta_from_hex(hex: &str) -> Bytes {
        Bytes::from(alloy::hex::decode(hex).unwrap())
    }

    fn build_fixed_price_meta(rainlang: &str) -> Bytes {
        build_rain_meta(rainlang.as_bytes())
    }

    /// Builds a minimal RainMetaDocumentV1 with a single CBOR item
    /// tagged as `RainlangSourceV1` (matching production metadata format).
    fn build_rain_meta(payload: &[u8]) -> Bytes {
        let mut buf = Vec::new();
        buf.extend_from_slice(&RAIN_META_DOCUMENT_V1_MAGIC);

        // CBOR map: key 0 = payload, key 1 = RainlangSourceV1 magic, key 2 = content type
        let map = ciborium::Value::Map(vec![
            (
                ciborium::Value::Integer(0.into()),
                ciborium::Value::Bytes(payload.to_vec()),
            ),
            (
                ciborium::Value::Integer(1.into()),
                ciborium::Value::Integer(RAINLANG_SOURCE_V1_MAGIC.into()),
            ),
            (
                ciborium::Value::Integer(2.into()),
                ciborium::Value::Text("application/octet-stream".to_owned()),
            ),
        ]);
        ciborium::into_writer(&map, &mut buf).unwrap();

        Bytes::from(buf)
    }

    // ── Metadata extraction ────────────────────────────────────

    #[test]
    fn extract_rainlang_from_real_metadata() {
        let meta = meta_from_hex(REAL_META_HEX);
        let source = extract_rainlang_source(&meta).unwrap();
        assert!(
            source.contains("max-output: max-value()"),
            "Expected max-value() in source, got: {source}"
        );
        assert!(
            source.contains("io: if("),
            "Expected if() in source, got: {source}"
        );
    }

    #[test]
    fn extract_fails_on_empty_metadata() {
        let result = extract_rainlang_source(&[]);
        assert!(matches!(result.unwrap_err(), ClassificationError::TooShort));
    }

    #[test]
    fn extract_fails_on_wrong_magic_prefix() {
        let result = extract_rainlang_source(&[0x00; 16]);
        assert!(matches!(
            result.unwrap_err(),
            ClassificationError::InvalidMagicPrefix
        ));
    }

    #[test]
    fn extract_from_constructed_metadata() {
        let meta = build_fixed_price_meta("_ _: 1000 185e18;:;");
        let source = extract_rainlang_source(&meta).unwrap();
        assert_eq!(source, "_ _: 1000 185e18;:;");
    }

    // ── Fixed-price classification ─────────────────────────────

    #[test]
    fn fixed_price_simple_literals() {
        let meta = build_fixed_price_meta("_ _: 1000 185e18;:;");
        let result = classify_order_metadata(&meta);
        assert!(
            matches!(result, OrderType::FixedPrice { .. }),
            "Expected FixedPrice, got: {result:?}"
        );
    }

    #[test]
    fn fixed_price_with_max_value() {
        let meta = build_fixed_price_meta("max-output: max-value(),\nio: 12;\n\n:;");
        let result = classify_order_metadata(&meta);
        assert!(
            matches!(result, OrderType::FixedPrice { .. }),
            "Expected FixedPrice, got: {result:?}"
        );
    }

    #[test]
    fn fixed_price_preserves_rainlang() {
        let source = "_ _: 1000 185e18;:;";
        let meta = build_fixed_price_meta(source);
        let result = classify_order_metadata(&meta);
        match result {
            OrderType::FixedPrice { rainlang } => {
                assert_eq!(rainlang, source);
            }
            other => panic!("Expected FixedPrice, got: {other:?}"),
        }
    }

    // ── Pyth oracle classification ─────────────────────────────

    #[test]
    fn pyth_oracle_detected_by_keyword() {
        let source = "using-words-from 0xabc\n\
                       max-output: max-value(),\n\
                       io: pyth-price(0xfeed);";
        let meta = build_rain_meta(source.as_bytes());
        let result = classify_order_metadata(&meta);
        assert!(
            matches!(result, OrderType::PythOracle { .. }),
            "Expected PythOracle, got: {result:?}"
        );
    }

    #[test]
    fn generic_oracle_is_unsupported() {
        let source = "max-output: oracle-read(),\nio: 100;";
        let meta = build_rain_meta(source.as_bytes());
        let result = classify_order_metadata(&meta);
        assert!(
            matches!(result, OrderType::Unsupported { .. }),
            "Generic oracle-read should be Unsupported, got: {result:?}"
        );
    }

    #[test]
    fn uniswap_twap_is_unsupported() {
        let source = "max-output: uniswap-v3-twap(0xpool),\nio: 100;";
        let meta = build_rain_meta(source.as_bytes());
        let result = classify_order_metadata(&meta);
        assert!(
            matches!(result, OrderType::Unsupported { .. }),
            "Uniswap TWAP should be Unsupported, got: {result:?}"
        );
    }

    // ── Unsupported patterns ───────────────────────────────────

    #[test]
    fn conditional_expression_is_unsupported() {
        // The real metadata has if(equal-to(...)) which is conditional
        let meta = meta_from_hex(REAL_META_HEX);
        let result = classify_order_metadata(&meta);
        assert!(
            matches!(result, OrderType::Unsupported { .. }),
            "Expected Unsupported for conditional expression, got: {result:?}"
        );
    }

    #[test]
    fn storage_read_is_unsupported() {
        let source = "max-output: get(key),\nio: 100;";
        let meta = build_rain_meta(source.as_bytes());
        let result = classify_order_metadata(&meta);
        assert!(
            matches!(result, OrderType::Unsupported { .. }),
            "Expected Unsupported for storage read, got: {result:?}"
        );
    }

    // ── Edge cases ─────────────────────────────────────────────

    #[test]
    fn empty_metadata_returns_unknown() {
        let result = classify_order_metadata(&Bytes::new());
        assert!(
            matches!(result, OrderType::Unknown),
            "Expected Unknown for empty metadata, got: {result:?}"
        );
    }

    #[test]
    fn strip_comments_removes_block_comments() {
        let input = "/* comment */ code /* another */ more";
        assert_eq!(strip_comments(input), " code  more");
    }

    #[test]
    fn strip_comments_preserves_code() {
        let input = "_ _: 1000 185e18;:;";
        assert_eq!(strip_comments(input), input);
    }

    #[test]
    fn strip_comments_removes_line_comments() {
        let input = "code // comment\nmore code";
        assert_eq!(strip_comments(input), "code \nmore code");
    }

    #[test]
    fn pyth_keyword_in_comment_is_not_matched() {
        let source = "// pyth-price is not used here\n_ _: 1000 185e18;:;";
        let meta = build_rain_meta(source.as_bytes());
        let result = classify_order_metadata(&meta);
        assert!(
            matches!(result, OrderType::FixedPrice { .. }),
            "Pyth keyword in comment should not trigger PythOracle, got: {result:?}"
        );
    }
}
