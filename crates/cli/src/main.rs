//! Command-line interface for manual trading and authentication operations.

mod cli;

use st0x_config::setup_tracing;
use st0x_hedge::install_tls_crypto_provider;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    install_tls_crypto_provider();

    let (ctx, command) = cli::CliEnv::parse_and_convert().await?;
    let _file_log_guard = setup_tracing(
        &ctx.log_level,
        ctx.log_format,
        ctx.file_logging.as_ref(),
        None,
        None,
    );

    // Surface the notices parsing collected, now that a subscriber exists.
    ctx.emit_startup_notices();

    Box::pin(cli::run_command(ctx, command)).await?;
    Ok(())
}
