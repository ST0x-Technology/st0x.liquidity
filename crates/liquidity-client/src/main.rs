//! Binary entrypoint: resolves the target environment, builds the auth backed
//! transport, and runs the parsed command through the library.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    st0x_liquidity_client::run().await
}

#[cfg(test)]
mod tests {
    /// The library's shown command test checks the program name against the
    /// package name, because Cargo sets the binary name only for binaries. This
    /// keeps the two equal, so a renamed binary fails here.
    #[test]
    fn binary_name_is_the_package_name() {
        assert_eq!(env!("CARGO_BIN_NAME"), env!("CARGO_PKG_NAME"));
    }
}
