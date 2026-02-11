use clap::Parser;
use st0x_hedge::config::{Ctx, Env};
use st0x_hedge::launch;
use st0x_hedge::setup_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Env { config, secrets } = Env::parse();
    let config = Ctx::load_file(&config, &secrets)?;

    let log_level: tracing::Level = (&config.log_level).into();

    let telemetry_guard = if let Some(ref telemetry) = config.telemetry {
        match telemetry.setup(log_level) {
            Ok(guard) => Some(guard),
            Err(e) => {
                eprintln!("Failed to setup telemetry: {e}");
                setup_tracing(&config.log_level);
                None
            }
        }
    } else {
        setup_tracing(&config.log_level);
        None
    };

    let result = launch(config).await;

    // Explicitly drop the telemetry guard to ensure TelemetryGuard::drop runs
    // before we return. Drop flushes pending spans and shuts down the tracer
    // provider, blocking until exports complete or timeout.
    drop(telemetry_guard);

    result?;
    Ok(())
}
