use clap::Parser;
use st0x_hedge::config::{Ctx, Env};
use st0x_hedge::run_bot_session;
use st0x_hedge::setup_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Env { config, secrets } = Env::parse();
    let ctx = Ctx::load_files(&config, &secrets).await?;

    let log_level: tracing::Level = (&ctx.log_level).into();

    let (_file_log_guard, telemetry_guard) = if let Some(ref telemetry) = ctx.telemetry {
        match telemetry.setup(log_level, ctx.log_dir.as_deref()) {
            Ok((file_guard, tele_guard)) => (file_guard, Some(tele_guard)),
            Err(error) => {
                eprintln!("Failed to setup telemetry: {error}");
                let file_guard = setup_tracing(&ctx.log_level, ctx.log_dir.as_deref());
                (file_guard, None)
            }
        }
    } else {
        let file_guard = setup_tracing(&ctx.log_level, ctx.log_dir.as_deref());
        (file_guard, None)
    };

    let result = run_bot_session(ctx).await;

    // Explicitly drop the telemetry guard to ensure TelemetryGuard::drop runs
    // before we return. Drop flushes pending spans and shuts down the tracer
    // provider, blocking until exports complete or timeout.
    drop(telemetry_guard);

    result?;
    Ok(())
}
