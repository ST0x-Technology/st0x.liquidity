use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    st0x_liquidity_client::run().await
}
