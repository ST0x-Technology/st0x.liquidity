//! Pre-deploy config validation binary.
//!
//! Parses both config and secrets TOML files and runs all validation
//! checks without starting the server or connecting to external services.
//! Exits 0 on success, 1 on validation failure.

use clap::Parser;

use st0x_hedge::config::{Ctx, Env};

fn main() -> std::process::ExitCode {
    let Env { config, secrets } = Env::parse();

    match Ctx::validate_files(&config, &secrets) {
        Ok(()) => {
            eprintln!("Config validation passed");
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Config validation failed: {error}");
            let mut source = std::error::Error::source(&error);
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            std::process::ExitCode::FAILURE
        }
    }
}
