//! Config validation binary.
//!
//! Parses the plaintext config file and runs every validation check the boot
//! path runs against it, without starting the server or reaching any external
//! service. Given `--secrets` as well, it also runs the checks that need the
//! two files together -- the deploy gate's mode. Without it the config half is
//! validated alone, which is what lets CI check every config the repository
//! ships without a secret, a network, or a clock.
//!
//! A config that names `[registry]` keeps its per-symbol tables in the
//! bucket. `--registry-file` supplies a local copy so they are checked too;
//! `--registry-state` checks persisted boot candidates. Without either option
//! the config is judged on its own, without network access. An explicit state
//! path with no seeded records reads the bucket, as deployment gates do.
//!
//! Exits 0 on success, 1 on validation failure.

use std::path::{Path, PathBuf};

use clap::Parser;

use st0x_config::{Ctx, CtxError, StartupNotice, TokenFile, fetch_gate_token_files};

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
    /// A local copy of the token file the config's `[registry]` names, so
    /// the per-symbol tables are checked too. Omit to judge the config alone.
    #[clap(long)]
    registry_file: Option<PathBuf>,
    /// Check the persisted pending and fallback copies, or the running copy.
    #[clap(long, conflicts_with = "registry_file")]
    registry_state: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let Args {
        config,
        secrets,
        registry_file,
        registry_state,
    } = Args::parse();
    let token_files = if registry_file.is_none() && registry_state.is_none() {
        vec![None]
    } else {
        match fetch_gate_token_files(&config, registry_file.as_deref(), registry_state.as_deref())
            .await
        {
            Ok(files) => files,
            Err(error) => {
                report_failure(&error);
                return std::process::ExitCode::FAILURE;
            }
        }
    };
    match validate_candidates(&config, secrets.as_deref(), &token_files) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            report_failure(&error);
            std::process::ExitCode::FAILURE
        }
    }
}

fn validate_candidates(
    config: &Path,
    secrets: Option<&Path>,
    files: &[Option<Vec<u8>>],
) -> Result<(), Box<CtxError>> {
    for bytes in files {
        let tokens = bytes
            .as_deref()
            .map_or(TokenFile::Skipped, TokenFile::Bytes);
        let (scope, validated) = secrets.map_or_else(
            || ("config", Ctx::validate_config_file(config, tokens)),
            |secrets| {
                (
                    "config and secrets",
                    Ctx::validate_files(config, secrets, tokens),
                )
            },
        );
        let notices = validated.map_err(Box::new)?;
        report_success(scope, config, &notices);
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_every_boot_candidate() {
        let config = Path::new("config/prod/st0x-hedge.toml");
        let valid = std::fs::read("tests/fixtures/tokens-production.toml").unwrap();
        validate_candidates(config, None, &[Some(valid.clone())]).unwrap();
        assert!(
            validate_candidates(
                config,
                None,
                &[Some(valid), Some(b"invalid TOML = [".to_vec())]
            )
            .is_err()
        );
    }
}
