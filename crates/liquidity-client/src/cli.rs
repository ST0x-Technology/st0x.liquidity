//! Command-line model for the liquidity client: the argument parser, the
//! command and resource enums, and their fixed API path mappings.

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::target::Env;

#[derive(Parser)]
#[command(
    name = "st0x-liquidity-client",
    about = "T0 liquidity bot operations client",
    version
)]
pub(crate) struct Cli {
    /// Target environment; selects the IAP-fronted API base URL.
    #[arg(long, value_enum)]
    pub(crate) env: Env,
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand)]
pub(crate) enum Command {
    /// Read-only queries against the liquidity bot.
    #[command(subcommand)]
    Read(Read),
    /// Safe recovery operations (debug tier).
    #[command(subcommand)]
    Debug(Debug),
}

#[derive(Subcommand)]
pub(crate) enum Read {
    /// Fetch a fixed read resource (pnl, trades, health, and so on).
    Resource(ResourceArgs),
    /// Lifecycle events for one trade aggregate.
    TradeEvents(TradeEventsArgs),
    /// Lifecycle events for one transfer aggregate.
    TransferEvents(TransferEventsArgs),
}

#[derive(Args)]
pub(crate) struct ResourceArgs {
    /// Resource to read.
    #[arg(value_enum)]
    pub(crate) resource: ReadResource,
    /// Extra query parameter, repeatable: --param key=value
    #[arg(long = "param", value_parser = parse_key_value)]
    pub(crate) params: Vec<(String, String)>,
}

#[derive(Args)]
pub(crate) struct TradeEventsArgs {
    /// Trading venue path segment.
    pub(crate) venue: String,
    /// Aggregate id path segment.
    pub(crate) aggregate_id: String,
    /// Extra query parameter, repeatable: --param key=value
    #[arg(long = "param", value_parser = parse_key_value)]
    pub(crate) params: Vec<(String, String)>,
}

#[derive(Args)]
pub(crate) struct TransferEventsArgs {
    /// Transfer kind path segment (for example mint or redemption).
    pub(crate) kind: String,
    /// Aggregate id path segment.
    pub(crate) aggregate_id: String,
    /// Extra query parameter, repeatable: --param key=value
    #[arg(long = "param", value_parser = parse_key_value)]
    pub(crate) params: Vec<(String, String)>,
}

#[derive(Subcommand)]
pub(crate) enum Debug {
    /// Resume interrupted mint and redemption transfers.
    Resume,
    /// Re-check a stuck transfer by kind and aggregate id.
    Recheck {
        /// Transfer kind path segment (for example mint or redemption).
        kind: String,
        /// Aggregate id path segment.
        id: String,
    },
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum ReadResource {
    Pnl,
    Trades,
    Transfers,
    OrdersPending,
    OrdersRaindex,
    Logs,
    Interrupted,
    Latencies,
    Rebalances,
    EquityRebalances,
    Reliability,
    Infra,
    Health,
}

impl ReadResource {
    pub(crate) fn path(self) -> &'static str {
        match self {
            Self::Pnl => "/pnl",
            Self::Trades => "/trades",
            Self::Transfers => "/transfers",
            Self::OrdersPending => "/orders/pending",
            Self::OrdersRaindex => "/orders/raindex",
            Self::Logs => "/logs",
            Self::Interrupted => "/transfers/interrupted",
            Self::Latencies => "/performance/latencies",
            Self::Rebalances => "/performance/rebalances",
            Self::EquityRebalances => "/performance/equity-rebalances",
            Self::Reliability => "/performance/reliability",
            Self::Infra => "/performance/infra",
            Self::Health => "/health",
        }
    }
}

fn parse_key_value(raw: &str) -> Result<(String, String), String> {
    match raw.split_once('=') {
        Some((key, value)) if !key.is_empty() => Ok((key.to_owned(), value.to_owned())),
        _ => Err(format!("expected key=value, got `{raw}`")),
    }
}

#[cfg(test)]
mod tests {
    //! Tests for CLI argument parsing and the key=value parameter parser.
    use clap::Parser as _;

    use super::{Cli, parse_key_value};
    use crate::target::Env;

    #[test]
    fn parses_key_and_value() {
        assert_eq!(
            parse_key_value("since=0"),
            Ok(("since".to_owned(), "0".to_owned()))
        );
    }

    #[test]
    fn rejects_empty_key() {
        assert_eq!(
            parse_key_value("=value"),
            Err("expected key=value, got `=value`".to_owned())
        );
    }

    #[test]
    fn keeps_equals_in_value() {
        assert_eq!(
            parse_key_value("filter=a=b"),
            Ok(("filter".to_owned(), "a=b".to_owned()))
        );
    }

    #[test]
    fn requires_explicit_env() {
        match Cli::try_parse_from(["st0x-liquidity-client", "read", "resource", "health"]) {
            Err(error) => {
                assert_eq!(
                    error.kind(),
                    clap::error::ErrorKind::MissingRequiredArgument
                );
            }
            Ok(_) => panic!("expected a missing --env error"),
        }
        let parsed = Cli::try_parse_from([
            "st0x-liquidity-client",
            "--env",
            "staging",
            "read",
            "resource",
            "health",
        ])
        .map(|cli| cli.env);
        assert!(matches!(parsed, Ok(Env::Staging)));
    }
}
