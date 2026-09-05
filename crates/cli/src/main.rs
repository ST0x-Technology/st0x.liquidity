//! Command-line interface for manual trading and authentication operations.

mod cli;

use st0x_config::setup_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (ctx, command) = cli::CliEnv::parse_and_convert().await?;
    let _file_log_guard = setup_tracing(
        &ctx.log_level,
        ctx.log_format,
        ctx.file_logging.as_ref(),
        None,
    );

    // Surface the notices parsing collected, now that a subscriber exists.
    ctx.emit_startup_notices();

    Box::pin(cli::run_command(ctx, command)).await?;
    Ok(())
}
