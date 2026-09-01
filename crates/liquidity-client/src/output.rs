//! Renders one JSON document to stdout as a single compact line, and the
//! failure type for that rendering.

use std::io::Write;

/// Failure while rendering output to stdout.
#[derive(Debug)]
pub enum OutputError {
    /// The value could not be serialized to JSON.
    Encode(serde_json::Error),
    /// Writing the rendered line to the output failed.
    Write(std::io::Error),
}

impl std::fmt::Display for OutputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode(source) => {
                write!(f, "the response could not be re-encoded as JSON: {source}")
            }
            Self::Write(source) => write!(f, "could not write output: {source}"),
        }
    }
}

impl std::error::Error for OutputError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Encode(source) => Some(source),
            Self::Write(source) => Some(source),
        }
    }
}

/// Renders `value` to `writer` as a single compact line, then flushes. Split
/// from `print` so write and flush failures are testable without stdout.
fn write_json<W: Write>(writer: &mut W, value: &serde_json::Value) -> Result<(), OutputError> {
    let rendered = serde_json::to_string(value).map_err(OutputError::Encode)?;
    writeln!(writer, "{rendered}").map_err(OutputError::Write)?;
    writer.flush().map_err(OutputError::Write)?;
    Ok(())
}

/// Writes one JSON document to stdout as a single compact line, so output is
/// stable and machine-readable.
pub fn print(value: &serde_json::Value) -> Result<(), OutputError> {
    write_json(&mut std::io::stdout().lock(), value)
}

#[cfg(test)]
mod tests {
    //! Tests for JSON rendering and output-write error propagation.
    use super::{OutputError, write_json};

    #[test]
    fn writes_compact_json_with_a_trailing_newline() {
        let mut buffer = Vec::new();
        let result = write_json(&mut buffer, &serde_json::json!({ "a": 1, "b": [2, 3] }));
        assert!(matches!(result, Ok(())));
        assert_eq!(buffer, b"{\"a\":1,\"b\":[2,3]}\n");
    }

    struct FailWriter;

    impl std::io::Write for FailWriter {
        fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("write refused"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::other("flush refused"))
        }
    }

    #[test]
    fn propagates_write_failures_as_output_error() {
        let mut writer = FailWriter;
        let result = write_json(&mut writer, &serde_json::json!({ "a": 1 }));
        assert!(matches!(result, Err(OutputError::Write(_))));
    }
}
