//! Fixtures and a dispatch entry point for tests in other crates that drive
//! the client against a real router. Compiled for this crate's own tests and
//! with `test-support`.

use clap::Parser;

pub use crate::auth::{StaticToken, TokenSource};
use crate::cli::Cli;
use crate::dispatch_value;
pub use crate::transport::{Client, TransportError};

/// Failure to parse or send one command through a supplied client.
#[derive(Debug)]
pub enum CommandError {
    /// The command line arguments are invalid.
    Parse(clap::Error),
    /// The API request failed.
    Transport(TransportError),
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(error) => write!(formatter, "{error}"),
            Self::Transport(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for CommandError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Parse(error) => Some(error),
            Self::Transport(error) => Some(error),
        }
    }
}

/// Parses and sends one command without resolving a deployment or printing.
///
/// `args` includes the program name and `--env`, as normal command line
/// arguments do. The supplied client decides the target and credentials.
pub async fn dispatch_args<A: TokenSource + Sync>(
    client: &Client<A>,
    args: &[&str],
) -> Result<serde_json::Value, CommandError> {
    let cli = Cli::try_parse_from(args).map_err(CommandError::Parse)?;
    dispatch_value(client, cli.command)
        .await
        .map_err(CommandError::Transport)
}

/// Role used by a command in the router regression suite.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandRole {
    /// A GET under `/liquidity-read`.
    Read,
    /// A POST under `/liquidity-write`.
    Write,
}

/// One complete command used to prove client and server route agreement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandCase {
    /// Role whose token the command must use.
    pub role: CommandRole,
    /// Full arguments including the program name and environment.
    pub args: Vec<&'static str>,
}

fn case(role: CommandRole, args: &[&'static str]) -> CommandCase {
    let mut full = vec!["st0x-liquidity-client", "--env", "staging"];
    full.extend_from_slice(args);
    CommandCase { role, args: full }
}

/// Every operation exposed by the client, with representative valid arguments.
pub fn command_cases() -> Vec<CommandCase> {
    let mut cases = [
        "pnl",
        "trades",
        "transfers",
        "orders-pending",
        "orders-raindex",
        "logs",
        "interrupted",
        "latencies",
        "rebalances",
        "equity-rebalances",
        "reliability",
        "infra",
        "health",
    ]
    .into_iter()
    .map(|resource| case(CommandRole::Read, &["read", "resource", resource]))
    .collect::<Vec<_>>();
    cases.extend([
        case(
            CommandRole::Read,
            &["read", "trade-events", "raindex", "abc"],
        ),
        case(
            CommandRole::Read,
            &["read", "transfer-events", "mint", "abc"],
        ),
        case(CommandRole::Write, &["debug", "resume"]),
        case(CommandRole::Write, &["debug", "recheck", "mint", "abc"]),
        case(
            CommandRole::Write,
            &["debug", "resume-usdc", "base-to-alpaca", "abc"],
        ),
        case(
            CommandRole::Write,
            &[
                "debug",
                "reconcile-usdc",
                "abc",
                "--reason",
                "funds-moved-manually",
            ],
        ),
        case(
            CommandRole::Write,
            &[
                "debug",
                "reconcile-equity",
                "mint",
                "abc",
                "--reason",
                "repaired",
            ],
        ),
        case(
            CommandRole::Write,
            &[
                "debug",
                "adopt-withdrawal",
                "abc",
                "--replacement-tx",
                "0xabc",
                "--reason",
                "repaired",
            ],
        ),
        case(
            CommandRole::Write,
            &["debug", "clear-pending-burn", "abc", "--reason", "dropped"],
        ),
        case(
            CommandRole::Write,
            &["debug", "fail-usdc-transfer", "abc", "--reason", "incident"],
        ),
        case(
            CommandRole::Write,
            &[
                "debug",
                "fail-equity-transfer",
                "mint",
                "abc",
                "--reason",
                "incident",
            ],
        ),
        case(
            CommandRole::Write,
            &[
                "debug",
                "position",
                "set",
                "AAPL",
                "--target-net",
                "1",
                "--reason",
                "correction",
            ],
        ),
        case(
            CommandRole::Write,
            &[
                "debug",
                "position",
                "release-hedge",
                "AAPL",
                "--order-id",
                "abc",
                "--reason",
                "correction",
            ],
        ),
        case(
            CommandRole::Write,
            &[
                "debug",
                "portfolio-snapshot",
                "set-mark",
                "--day",
                "2026-09-01",
                "--symbol",
                "AAPL",
                "--usd-mark",
                "1",
                "--observed-at",
                "2026-09-01T20:00:00Z",
                "--source",
                "operator",
                "--reason",
                "correction",
            ],
        ),
        case(CommandRole::Write, &["debug", "process-tx", "0xabc"]),
        case(
            CommandRole::Write,
            &["debug", "view", "rebuild", "position", "--id", "AAPL"],
        ),
        case(
            CommandRole::Write,
            &[
                "debug",
                "cctp",
                "complete-mint",
                "--burn-tx",
                "0xabc",
                "--source-chain",
                "base",
            ],
        ),
    ]);
    cases.extend(capital_command_cases());
    cases
}

fn capital_command_cases() -> [CommandCase; 7] {
    const TOKEN: &str = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913";
    const VAULT: &str = "0x00000000000000000000000000000000000000000000000000000000000000a1";
    const OPERATION: &str = "6f1c2a1e-6c39-4a77-9a8e-1f0b7d6e8c11";

    [
        case(
            CommandRole::Write,
            &[
                "capital",
                "transfer-usdc",
                "--direction",
                "to-raindex",
                "--amount",
                "1",
            ],
        ),
        case(
            CommandRole::Write,
            &[
                "capital",
                "vault-deposit",
                "--amount",
                "1",
                "--token",
                TOKEN,
                "--vault-id",
                VAULT,
            ],
        ),
        case(
            CommandRole::Write,
            &[
                "capital",
                "vault-withdraw",
                "--amount",
                "1",
                "--token",
                TOKEN,
                "--vault-id",
                VAULT,
            ],
        ),
        case(
            CommandRole::Write,
            &["capital", "vault-withdraw-usdc", "--amount", "1"],
        ),
        case(
            CommandRole::Write,
            &[
                "capital",
                "cctp-bridge",
                "--from",
                "base",
                "--amount",
                "1",
                "--operation-id",
                OPERATION,
            ],
        ),
        case(
            CommandRole::Write,
            &[
                "capital",
                "cctp-burn-supersede",
                "--operation-id",
                OPERATION,
                "--superseding-tx",
                "0xabc",
            ],
        ),
        case(CommandRole::Write, &["capital", "reset-allowance"]),
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use clap::{CommandFactory, ValueEnum};

    use super::command_cases;
    use crate::cli::{Cli, ReadResource};

    /// Collects the subcommand path of every command that takes no further
    /// subcommand.
    fn leaf_commands(
        command: &clap::Command,
        path: &mut Vec<String>,
        leaves: &mut BTreeSet<Vec<String>>,
    ) {
        let mut subcommands = command.get_subcommands().peekable();
        if subcommands.peek().is_none() {
            leaves.insert(path.clone());
            return;
        }
        for subcommand in subcommands {
            path.push(subcommand.get_name().to_owned());
            leaf_commands(subcommand, path, leaves);
            path.pop();
        }
    }

    /// A new client verb without a case would escape the server route test, so
    /// every leaf of the clap tree and every read resource must have one.
    #[test]
    fn command_cases_cover_every_leaf_command() {
        let mut leaves = BTreeSet::new();
        leaf_commands(&Cli::command(), &mut Vec::new(), &mut leaves);
        assert!(leaves.iter().all(|leaf| !leaf.is_empty()));

        let cases = command_cases();
        let uncovered = leaves
            .iter()
            .filter(|leaf| {
                !cases.iter().any(|case| {
                    case.args.get(3..).is_some_and(|verb| {
                        verb.len() >= leaf.len()
                            && leaf.iter().zip(verb).all(|(name, arg)| name == arg)
                    })
                })
            })
            .collect::<Vec<_>>();
        assert!(
            uncovered.is_empty(),
            "commands without a case: {uncovered:?}"
        );

        for resource in ReadResource::value_variants() {
            let name = resource
                .to_possible_value()
                .expect("no read resource is skipped")
                .get_name()
                .to_owned();
            assert!(
                cases.iter().any(|case| case
                    .args
                    .get(3..)
                    .is_some_and(|verb| verb == ["read", "resource", name.as_str()])),
                "read resource {name} has no case"
            );
        }
    }
}
