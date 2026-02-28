// Proc macro crates must panic on internal errors (malformed TokenStream
// generation) -- there is no caller to return Result to. These panics
// surface as compile errors for the user.
#![allow(clippy::expect_used)]

//! Proc macro for compile-time `Float` literal parsing.
//!
//! Evaluates `Float::parse` at compile time via revm when given a numeric
//! literal, emitting a const `Float::from_raw(FixedBytes([...]))` expression.
//! Invalid literals become compile errors instead of runtime panics.
//!
//! For runtime expressions (variables, method calls), falls back to
//! `Float::parse` at runtime with a panic on failure.

use proc_macro::{TokenStream, TokenTree};
use rain_math_float::Float;

/// Returns true if the string looks like a numeric literal
/// (optional `-`, then digits, optional `.` and more digits).
fn is_numeric_literal_str(input: &str) -> bool {
    let trimmed = input.trim();

    // Must match: optional minus, digits, optional decimal point + digits
    let s = trimmed.strip_prefix('-').unwrap_or(trimmed);

    if s.is_empty() {
        return false;
    }

    let mut seen_dot = false;
    for ch in s.chars() {
        if ch == '.' {
            if seen_dot {
                return false;
            }
            seen_dot = true;
        } else if !ch.is_ascii_digit() {
            return false;
        }
    }

    true
}

/// Parses a numeric literal into a `Float` at compile time.
///
/// Accepts bare numeric literals -- no quotes needed:
///
/// ```ignore
/// use st0x_float_macro::float;
///
/// let value = float!(1.5);
/// let zero = float!(0);
/// let negative = float!(-42.7);
/// ```
///
/// Also accepts runtime expressions, falling back to `Float::parse` at
/// runtime:
///
/// ```ignore
/// let price = 42.5_f64;
/// let value = float!(&price.to_string());
/// ```
///
/// Invalid numeric literals produce a compile error:
///
/// ```ignore,compile_fail
/// let bad = float!(99999999999999999999999999999999999999999999999999999999999999999999999);
/// ```
#[proc_macro]
pub fn float(input: TokenStream) -> TokenStream {
    let tokens: Vec<TokenTree> = input.into_iter().collect();

    // Reconstruct the literal from tokens. The Rust tokenizer splits
    // `-1.4` into separate tokens (`-`, `1.4`), so we join them without
    // spaces to recover the original literal.
    let joined: String = tokens.iter().map(ToString::to_string).collect();

    if is_numeric_literal_str(&joined) {
        compile_time_float(&joined)
    } else {
        // Runtime fallback: emit Float::parse($expr.to_string()).unwrap()
        let expr: TokenStream = tokens.into_iter().collect();
        let expr_str = expr.to_string();

        format!(
            "match ::rain_math_float::Float::parse(({expr_str}).to_string()) {{ \
                Ok(value) => value, \
                Err(error) => panic!(\"float!({{}}) failed: {{error}}\", {expr_str:?}), \
            }}"
        )
        .parse()
        .expect("runtime float fallback TokenStream parse failed")
    }
}

/// Parses a numeric literal string into a `TokenStream` that constructs
/// a `Float` at compile time via `Float::from_raw`.
fn compile_time_float(literal: &str) -> TokenStream {
    let parsed = match Float::parse(literal.to_string()) {
        Ok(value) => value,
        Err(error) => {
            let message = format!("float!({literal}) failed: {error}");
            return format!("compile_error!({message:?})")
                .parse()
                .expect("compile_error! TokenStream parse failed");
        }
    };

    let bytes = parsed.get_inner().0;
    let byte_tokens: Vec<String> = bytes.iter().map(|byte| format!("{byte:#04x}")).collect();
    let bytes_list = byte_tokens.join(", ");

    format!("rain_math_float::Float::from_raw(alloy_primitives::FixedBytes([{bytes_list}]))")
        .parse()
        .expect("generated Float::from_raw TokenStream parse failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_literals_are_numeric() {
        assert!(is_numeric_literal_str("0"));
        assert!(is_numeric_literal_str("1"));
        assert!(is_numeric_literal_str("42"));
        assert!(is_numeric_literal_str("100000"));
    }

    #[test]
    fn decimal_literals_are_numeric() {
        assert!(is_numeric_literal_str("1.5"));
        assert!(is_numeric_literal_str("0.001"));
        assert!(is_numeric_literal_str("155.00"));
        assert!(is_numeric_literal_str("123.456789"));
    }

    #[test]
    fn negative_literals_are_numeric() {
        assert!(is_numeric_literal_str("-1"));
        assert!(is_numeric_literal_str("-42.7"));
        assert!(is_numeric_literal_str("-0.5"));
    }

    #[test]
    fn non_numeric_inputs_are_rejected() {
        assert!(!is_numeric_literal_str(""));
        assert!(!is_numeric_literal_str("-"));
        assert!(!is_numeric_literal_str("abc"));
        assert!(!is_numeric_literal_str("1.2.3"));
        assert!(!is_numeric_literal_str("i64::MAX"));
        assert!(!is_numeric_literal_str("&price.to_string()"));
        assert!(!is_numeric_literal_str("some_var"));
    }

    #[test]
    fn edge_case_dot_handling() {
        // Trailing dot passes the literal check (Rust tokenizer accepts `5.`
        // as a float literal), but Float::parse will reject it at compile time
        assert!(is_numeric_literal_str("5."));

        // Lone dot is accepted by our check since it has a dot and no
        // non-digit chars, but Float::parse rejects it
        assert!(is_numeric_literal_str("."));

        // Leading dot (no integer part)
        assert!(is_numeric_literal_str(".5"));
    }

    #[test]
    fn signs_and_whitespace() {
        // Plus sign is not accepted (only minus)
        assert!(!is_numeric_literal_str("+1"));

        // Double minus
        assert!(!is_numeric_literal_str("--1"));

        // Minus in middle
        assert!(!is_numeric_literal_str("1-2"));

        // Whitespace is trimmed, so padded numbers pass
        assert!(is_numeric_literal_str(" 42 "));
        assert!(is_numeric_literal_str(" -3.14 "));
    }

    #[test]
    fn underscores_and_scientific_notation_rejected() {
        // Rust numeric separators are not accepted
        assert!(!is_numeric_literal_str("1_000"));

        // Scientific notation is not accepted
        assert!(!is_numeric_literal_str("1e5"));
        assert!(!is_numeric_literal_str("1.5e10"));

        // Hex is not accepted
        assert!(!is_numeric_literal_str("0xff"));
    }
}
