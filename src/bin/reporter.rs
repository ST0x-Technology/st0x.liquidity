use clap::Parser;
use st0x_hedge::config::{Env, setup_tracing};
use st0x_hedge::reporter::{self, ReporterConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env = Env::parse();
    let config = ReporterConfig::load_file(&env.config_file)?;
    setup_tracing(&config.log_level());

    reporter::run(config).await
}
