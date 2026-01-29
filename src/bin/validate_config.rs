use clap::Parser;
use std::process::ExitCode;

use st0x_hedge::env::{ConfigError, Env};

fn main() -> ExitCode {
    let env = Env::parse();
    match st0x_hedge::env::Config::load_file(&env.config_file) {
        Ok(_) => {
            eprintln!("Config validation passed");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("Config validation failed: {}", error_kind(&e));
            ExitCode::FAILURE
        }
    }
}

fn error_kind(e: &ConfigError) -> &'static str {
    match e {
        ConfigError::Rebalancing(_) => "rebalancing configuration error",
        ConfigError::MissingOrderOwner => "ORDER_OWNER required when rebalancing is disabled",
        ConfigError::PrivateKeyDerivation(_) => "failed to derive address from EVM_PRIVATE_KEY",
        ConfigError::Io(_) => "failed to read config file",
        ConfigError::Toml(_) => "failed to parse config file",
    }
}
