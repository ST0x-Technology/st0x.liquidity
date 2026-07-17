//! Pre-deploy Turnkey approval-policy coverage verification binary.

use clap::Parser;

use st0x_config::Env;
use st0x_hedge::approval_policy::{ApprovalPolicyVerification, verify_turnkey_approval_policies};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let Env { config, secrets } = Env::parse();

    match verify_turnkey_approval_policies(&config, &secrets).await {
        Ok(ApprovalPolicyVerification::SkippedNonTurnkey) => {
            eprintln!("Turnkey approval policy verification skipped for non-Turnkey wallet");
            std::process::ExitCode::SUCCESS
        }
        Ok(ApprovalPolicyVerification::Verified {
            target_count,
            policy_count,
        }) => {
            eprintln!(
                "Turnkey approval policy verification passed: {target_count} startup targets \
                 covered by {policy_count} policies"
            );
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Turnkey approval policy verification failed: {error}");
            let mut source = std::error::Error::source(&error);
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            std::process::ExitCode::FAILURE
        }
    }
}
