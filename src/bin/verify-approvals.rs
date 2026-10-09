//! Pre-deploy Turnkey approval-policy coverage verification binary.

use clap::Parser;

use st0x_config::{Env, TokenFile, fetch_gate_token_files};
use st0x_hedge::approval_policy::{ApprovalPolicyVerification, verify_turnkey_approval_policies};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let Env {
        config,
        secrets,
        registry_file,
        registry_state,
    } = Env::parse();

    let token_files =
        match fetch_gate_token_files(&config, registry_file.as_deref(), registry_state.as_deref())
            .await
        {
            Ok(files) => files,
            Err(error) => {
                eprintln!("Failed to load registry deploy inputs: {error}");
                return std::process::ExitCode::FAILURE;
            }
        };
    for bytes in &token_files {
        let tokens = bytes
            .as_deref()
            .map_or(TokenFile::Skipped, TokenFile::Bytes);
        if verify(&config, &secrets, tokens).await == std::process::ExitCode::FAILURE {
            return std::process::ExitCode::FAILURE;
        }
    }
    std::process::ExitCode::SUCCESS
}

async fn verify(
    config: &std::path::Path,
    secrets: &std::path::Path,
    tokens: TokenFile<'_>,
) -> std::process::ExitCode {
    match verify_turnkey_approval_policies(config, secrets, tokens).await {
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
