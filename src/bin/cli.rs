use st0x_hedge::cli;
use st0x_hedge::config::setup_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (config, command) = cli::CliEnv::parse_and_convert()?;
    setup_tracing(&config.log_level);

    cli::run_command(config, command).await?;
    Ok(())
}
