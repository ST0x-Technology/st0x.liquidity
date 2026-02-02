use std::process::ExitCode;

use st0x_hedge::env::validate_config;

fn main() -> ExitCode {
    validate_config()
}
