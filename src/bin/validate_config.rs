use clap::Parser;
use std::process::ExitCode;

use st0x_hedge::env::{ConfigError, Env};

fn main() -> ExitCode {
    match Env::try_parse() {
        Ok(env) => match env.into_config() {
            Ok(_) => {
                eprintln!("Config validation passed");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("Config validation failed: {}", error_kind(&e));
                ExitCode::FAILURE
            }
        },
        Err(e) => {
            eprintln!("Config validation failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn error_kind(e: &ConfigError) -> &'static str {
    match e {
        ConfigError::Rebalancing(_) => "rebalancing configuration error",
        ConfigError::Clap(_) => "missing or invalid environment variable",
        ConfigError::MissingOrderOwner => "ORDER_OWNER required when rebalancing is disabled",
        ConfigError::PrivateKeyDerivation(_) => "failed to derive address from EVM_PRIVATE_KEY",
        ConfigError::InvalidThreshold(_) => "invalid execution threshold",
    }
}
