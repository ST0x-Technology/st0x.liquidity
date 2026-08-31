//! Renders one JSON document to stdout as a single compact line, and the
//! failure type for that rendering.

use std::io::Write;

/// Failure while rendering output to stdout.
#[derive(Debug)]
pub enum OutputError {
    /// The value could not be serialized to JSON.
    Encode(serde_json::Error),
    /// Writing the rendered line to stdout failed.
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

/// Writes one JSON document to stdout as a single compact line, so output is
/// stable and machine-readable.
pub fn print(value: &serde_json::Value) -> Result<(), OutputError> {
    let rendered = serde_json::to_string(value).map_err(OutputError::Encode)?;
    let mut out = std::io::stdout().lock();
    writeln!(out, "{rendered}").map_err(OutputError::Write)?;
    out.flush().map_err(OutputError::Write)?;
    Ok(())
}
