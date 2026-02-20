use clap::Parser;
use st0x_hedge::config::{Ctx, Env};
use st0x_hedge::launch;
use st0x_hedge::setup_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Env { config, secrets } = Env::parse();
    let ctx = Ctx::load_files(&config, &secrets).await?;

    let log_level: tracing::Level = (&ctx.log_level).into();

    let telemetry_guard = if let Some(ref telemetry) = ctx.telemetry {
        match telemetry.setup(log_level) {
            Ok(guard) => Some(guard),
            Err(error) => {
                eprintln!("Failed to setup telemetry: {error}");
                setup_tracing(&ctx.log_level);
                None
            }
        }
    } else {
        setup_tracing(&ctx.log_level);
        None
    };

    let result = launch(ctx).await;

    // Explicitly drop the telemetry guard to ensure TelemetryGuard::drop runs
    // before we return. Drop flushes pending spans and shuts down the tracer
    // provider, blocking until exports complete or timeout.
    drop(telemetry_guard);

    result?;
    Ok(())
}
