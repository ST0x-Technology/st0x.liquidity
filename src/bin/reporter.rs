use clap::Parser;
use st0x_hedge::reporter::{self, ReporterConfig, ReporterEnv};
use st0x_hedge::setup_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ReporterEnv { config } = ReporterEnv::parse();
    let config = ReporterConfig::load_file(&config)?;
    setup_tracing(&config.log_level());

    reporter::run(config).await
}
