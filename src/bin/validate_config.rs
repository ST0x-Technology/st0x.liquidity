use std::process::ExitCode;

use st0x_hedge::env::validate_config;

fn main() -> ExitCode {
    tracing_subscriber::fmt::init();
    validate_config()
}
