use clap::Parser;
use std::sync::LazyLock;

use st0x_config::{Ctx, Env, TokenSource, claim_boot_tokens};
use st0x_hedge::{
    PROCESS_START, activate_log_counts, apalis_board_tracing_layer, install_metrics_recorder,
    install_tls_crypto_provider, report_registry_boot, run_server_bot_session, setup_tracing,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    LazyLock::force(&PROCESS_START);
    install_tls_crypto_provider();

    let Env {
        config,
        secrets,
        registry_file,
        ..
    } = Env::parse();
    let claim = claim_boot_tokens(&config, registry_file.as_deref()).await?;
    let ctx = Ctx::load_files(
        &config,
        &secrets,
        TokenSource::Claimed(claim.tokens.as_deref()),
    )
    .await?;

    let log_level: tracing::Level = (&ctx.log_level).into();
    // Installed before the subscriber exists, so `log_events_total` also
    // counts the warnings logged while the bot boots.
    install_metrics_recorder()?;
    // Activated before the subscriber exists, so every file event of this
    // process is counted live and the seed reads only the bytes the log files
    // held before it.
    let log_sink = ctx
        .file_logging
        .as_ref()
        .map(|file_logging| activate_log_counts(file_logging.directory()));

    let (file_log_guard, telemetry_guard) = if let Some(ref telemetry) = ctx.telemetry {
        match telemetry.setup(
            log_level,
            ctx.log_format,
            ctx.file_logging.as_ref(),
            Some(apalis_board_tracing_layer(log_level)),
            log_sink.clone(),
        ) {
            Ok((file_guard, tele_guard)) => (file_guard, Some(tele_guard)),
            Err(error) => {
                eprintln!("Failed to setup telemetry: {error}");
                let file_guard = setup_tracing(
                    &ctx.log_level,
                    ctx.log_format,
                    ctx.file_logging.as_ref(),
                    Some(apalis_board_tracing_layer(log_level)),
                    log_sink,
                );
                (file_guard, None)
            }
        }
    } else {
        let file_guard = setup_tracing(
            &ctx.log_level,
            ctx.log_format,
            ctx.file_logging.as_ref(),
            Some(apalis_board_tracing_layer(log_level)),
            log_sink,
        );
        (file_guard, None)
    };

    // Now that a subscriber exists, surface the notices parsing collected
    // (deprecation shims, absent optional sections). During Ctx::load_files
    // there was no subscriber, so logging there would have been dropped.
    ctx.emit_startup_notices();
    report_registry_boot(&claim.outcome);

    let result = run_server_bot_session(ctx, claim.booted).await;

    // Explicitly drop the telemetry guard to ensure TelemetryGuard::drop runs
    // before we return. Drop flushes pending spans and shuts down the tracer
    // provider, blocking until exports complete or timeout.
    drop(telemetry_guard);

    if result? == st0x_hedge::ShutdownReason::Reload {
        drop(file_log_guard);
        std::process::exit(75);
    }
    Ok(())
}
