//! Verifies migrations and event-replay compatibility against a real
//! (prod/staging) database, without ever mutating it.
//!
//! Used both as a pre-deploy gate (invoked from the NixOS activation script
//! against the live database once its writer is stopped, see `deploy.nix`)
//! and manually while developing a migration, e.g. against a downloaded
//! snapshot:
//!
//! ```text
//! prod-status
//! cargo run --bin verify-migrations -- --db ./claude-local-ctx/<ts>-running/st0x-hedge.db
//! ```
//!
//! Exits 0 if migrations applied cleanly and every persisted aggregate
//! still replays under current code; exits 1 otherwise.

use std::path::PathBuf;

use clap::Parser;

use st0x_hedge::migration_verification::verify_migrations;

#[derive(Parser)]
struct Args {
    /// Path to the SQLite database to verify against. Never modified -- a
    /// disposable scratch copy is made internally via `VACUUM INTO`.
    #[arg(long)]
    db: PathBuf,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let Args { db } = Args::parse();

    match verify_migrations(&db).await {
        Ok(report) => {
            print!("{report}");
            if report.has_failures() {
                eprintln!(
                    "Verification FAILED: some aggregates cannot replay under current code. \
                     A repair/backfill migration may be needed -- see \
                     migrations/20260701223808_repair_legacy_usdc_conversion_confirmed_events.sql \
                     for a template."
                );
                std::process::ExitCode::FAILURE
            } else {
                eprintln!("Verification passed.");
                std::process::ExitCode::SUCCESS
            }
        }
        Err(error) => {
            eprintln!("Migration verification failed: {error}");
            let mut source = std::error::Error::source(&error);
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            std::process::ExitCode::FAILURE
        }
    }
}
