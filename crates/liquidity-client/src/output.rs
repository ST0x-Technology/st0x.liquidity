use crate::error::Error;

/// Writes one JSON document to stdout as a single compact line, so output is
/// stable and machine-readable.
pub fn print(value: &serde_json::Value) -> Result<(), Error> {
    let rendered = serde_json::to_string(value).map_err(Error::Encode)?;
    println!("{rendered}");
    Ok(())
}
