//! Verifies migrations and event-replay compatibility against a real
//! (prod/staging) database, without ever mutating it.
//!
//! Used both as a pre-deploy gate (invoked from the NixOS activation script
//! against the live database once its writer is stopped, see `deploy.nix`)
//! and manually while developing a migration, e.g. against a downloaded
//! snapshot:
//!
//! ```text
//! snapshot="$(nix run .#prodDbSnapshot)"
//! cargo run --bin verify-migrations -- --db "$snapshot" \
//!   --config config/prod/st0x-hedge.toml
//! ```
//!
//! Exits 0 if migrations applied cleanly and every persisted aggregate
//! still replays under current code; exits 1 otherwise.

use std::path::PathBuf;

use clap::Parser;

use st0x_config::{DeploymentSymbolPolicy, load_deployment_symbol_policy};
use st0x_hedge::migration_verification::verify_migrations;

#[derive(Parser)]
struct Args {
    /// Path to the SQLite database to verify against. Omit only on a first
    /// deploy, when no durable state exists yet. Never modified -- a disposable
    /// scratch copy is made internally via `VACUUM INTO`.
    #[arg(long)]
    db: Option<PathBuf>,
    /// Candidate plaintext config whose equity symbols must be compatible
    /// with the durable state in `db`.
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let Args { db, config } = Args::parse();

    let symbol_policy = match load_deployment_symbol_policy(&config) {
        Ok(policy) => policy,
        Err(error) => {
            eprintln!("Failed to load deployment symbol policy: {error}");
            print_error_sources(&error);
            return std::process::ExitCode::FAILURE;
        }
    };

    let Some(db) = db else {
        return verify_first_deploy(&symbol_policy);
    };

    match verify_migrations(&db, &symbol_policy).await {
        Ok(report) => {
            print!("{report}");
            if report.has_failures() {
                eprintln!(
                    "Verification FAILED: see the diagnostics above. Restore accidentally \
                     removed symbols, mark intentional retirements, or add a repair/backfill \
                     migration for aggregate replay failures."
                );
                std::process::ExitCode::FAILURE
            } else {
                eprintln!("Verification passed.");
                std::process::ExitCode::SUCCESS
            }
        }
        Err(error) => {
            eprintln!("Migration verification failed: {error}");
            print_error_sources(&error);
            std::process::ExitCode::FAILURE
        }
    }
}

fn verify_first_deploy(policy: &DeploymentSymbolPolicy) -> std::process::ExitCode {
    if policy.retired().is_empty() {
        eprintln!("Verification passed: no database or durable symbol references exist yet.");
        return std::process::ExitCode::SUCCESS;
    }

    eprintln!("Verification FAILED: retired_symbols cannot be set before durable state exists.");
    for symbol in policy.retired() {
        eprintln!("  - {symbol}: listed in retired_symbols but has no durable reference");
    }
    std::process::ExitCode::FAILURE
}

fn print_error_sources(error: &dyn std::error::Error) {
    let mut source = error.source();
    while let Some(cause) = source {
        eprintln!("  caused by: {cause}");
        source = cause.source();
    }
}

#[cfg(test)]
mod tests {
    use st0x_config::DeploymentSymbolPolicy;
    use st0x_execution::Symbol;

    use super::*;

    #[test]
    fn first_deploy_accepts_an_explicit_empty_retirement_policy() {
        let policy = DeploymentSymbolPolicy::new([], []).unwrap();

        assert_eq!(
            verify_first_deploy(&policy),
            std::process::ExitCode::SUCCESS
        );
    }

    #[test]
    fn first_deploy_rejects_a_retirement_without_durable_state() {
        let policy = DeploymentSymbolPolicy::new([], [Symbol::new("QSEP").unwrap()]).unwrap();

        assert_eq!(
            verify_first_deploy(&policy),
            std::process::ExitCode::FAILURE
        );
    }
}
