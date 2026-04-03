//! Decodes Rain Float hex values from stdin to human-readable decimals.
//!
//! Reads one hex string per line from stdin and writes the decoded
//! decimal value to stdout. Non-hex lines are passed through unchanged.
//!
//! Used by `prod-status` to decode Raindex subgraph vault balances.

use std::io::{BufRead, Write};

use rain_math_float::Float;
use st0x_float_serde::format_float_with_fallback;

fn main() {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(error) => {
                eprintln!("read error: {error}");
                std::process::exit(1);
            }
        };

        let trimmed = line.trim();

        let decoded =
            Float::from_hex(trimmed).map_or(line, |float| format_float_with_fallback(&float));

        if writeln!(out, "{decoded}").is_err() {
            break;
        }
    }
}
