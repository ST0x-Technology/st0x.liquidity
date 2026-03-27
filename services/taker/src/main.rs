//! Binary entry point for the order taker bot.

use clap::Parser;
use st0x_taker::{Env, launch, setup_tracing};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Env { config, secrets } = Env::parse();
    let ctx = st0x_taker::Ctx::load_files(&config, &secrets).await?;

    setup_tracing(&ctx.log_level);

    let market_data = ctx.build_market_data()?;

    launch(ctx, market_data).await
}
