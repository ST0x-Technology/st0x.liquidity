use clap::Parser;
use st0x_hedge::env::{Config, Env, setup_tracing};
use st0x_hedge::launch;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let parsed_env = Env::parse();
    let config = Config::load_file(&parsed_env.config_file)?;

    let telemetry_guard = if let Some(ref hyperdx) = config.hyperdx {
        match hyperdx.setup_telemetry() {
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
