//! Config validation binary.
//!
//! Parses the plaintext config file and runs every validation check the boot
//! path runs against it, without starting the server or reaching any external
//! service. Given `--secrets` as well, it also runs the checks that need the
//! two files together -- the deploy gate's mode. Without it the config half is
//! validated alone, which is what lets CI check every config the repository
//! ships without a secret, a network, or a clock.
//!
//! Exits 0 on success, 1 on validation failure.

use std::path::{Path, PathBuf};

use clap::Parser;

use st0x_config::{Ctx, CtxError, StartupNotice};

#[derive(Parser, Debug)]
#[command(
    about = "Validate a st0x-hedge config file",
    long_about = "Validates a plaintext config TOML. With --secrets, additionally runs the \
                  config/secrets cross-checks the deploy gate runs (broker credentials, \
                  per-chain rpc_url, wallet keys, pricing and issuance API keys). Without \
                  it, only the config file is judged -- enough for CI, which has no secrets."
)]
struct Args {
    /// Path to the plaintext TOML configuration file
    #[clap(long)]
    config: PathBuf,
    /// Path to the decrypted TOML secrets file. Omit to validate the config
    /// file on its own.
    #[clap(long)]
    secrets: Option<PathBuf>,
}

fn main() -> std::process::ExitCode {
    let Args { config, secrets } = Args::parse();

    let (scope, validated) = secrets.as_ref().map_or_else(
        || ("config", Ctx::validate_config_file(&config)),
        |secrets| ("config and secrets", Ctx::validate_files(&config, secrets)),
    );

    match validated {
        Ok(startup_notices) => {
            report_success(scope, &config, &startup_notices);
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            report_failure(&error);
            std::process::ExitCode::FAILURE
        }
    }
}

/// The plain-text report: no tracing subscriber exists in this binary, so the
/// notices collected during parsing are printed here or not at all.
fn report_success(scope: &str, config: &Path, startup_notices: &[StartupNotice]) {
    for notice in startup_notices {
        println!("{notice}");
    }

    println!("{scope} validation passed: {}", config.display());
}

fn report_failure(error: &CtxError) {
    eprintln!("Config validation failed: {error}");

    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        eprintln!("  caused by: {cause}");
        source = cause.source();
    }
}
