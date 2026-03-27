//! Command-line interface for manual trading and authentication operations.

use st0x_hedge::cli;
use st0x_hedge::setup_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (ctx, command) = cli::CliEnv::parse_and_convert().await?;
    setup_tracing(&ctx.log_level);

    Box::pin(cli::run_command(ctx, command)).await?;
    Ok(())
}
