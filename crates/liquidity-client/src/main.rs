//! Binary entrypoint: resolves the target environment, builds the auth backed
//! transport, and runs the parsed command through the library.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    st0x_liquidity_client::run().await
}
