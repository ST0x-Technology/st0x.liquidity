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
pub struct Cli {
    /// Target environment; selects the IAP-fronted API base URL.
    #[arg(long, value_enum, global = true, default_value = "staging")]
    pub env: Env,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Read-only queries against the liquidity bot.
    #[command(subcommand)]
    Read(Read),
    /// Safe recovery operations (debug tier).
    #[command(subcommand)]
    Debug(Debug),
}

#[derive(Subcommand)]
pub enum Read {
    /// Fetch a fixed read resource (pnl, trades, health, and so on).
    Resource(ResourceArgs),
    /// Lifecycle events for one trade aggregate.
    TradeEvents(TradeEventsArgs),
    /// Lifecycle events for one transfer aggregate.
    TransferEvents(TransferEventsArgs),
}

#[derive(Args)]
pub struct ResourceArgs {
    /// Resource to read.
    #[arg(value_enum)]
    pub resource: ReadResource,
    /// Extra query parameter, repeatable: --param key=value
    #[arg(long = "param", value_parser = parse_key_value)]
    pub params: Vec<(String, String)>,
}

#[derive(Args)]
pub struct TradeEventsArgs {
    /// Trading venue path segment.
    pub venue: String,
    /// Aggregate id path segment.
    pub aggregate_id: String,
    /// Extra query parameter, repeatable: --param key=value
    #[arg(long = "param", value_parser = parse_key_value)]
    pub params: Vec<(String, String)>,
}

#[derive(Args)]
pub struct TransferEventsArgs {
    /// Transfer kind path segment (for example mint or redemption).
    pub kind: String,
    /// Aggregate id path segment.
    pub aggregate_id: String,
    /// Extra query parameter, repeatable: --param key=value
    #[arg(long = "param", value_parser = parse_key_value)]
    pub params: Vec<(String, String)>,
}

#[derive(Subcommand)]
pub enum Debug {
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
pub enum ReadResource {
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
    pub fn path(self) -> &'static str {
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
    use super::parse_key_value;

    #[test]
    fn parses_key_and_value() {
        assert_eq!(
            parse_key_value("since=0"),
            Ok(("since".to_owned(), "0".to_owned()))
        );
    }

    #[test]
    fn rejects_empty_key() {
        assert!(parse_key_value("=value").is_err());
    }

    #[test]
    fn keeps_equals_in_value() {
        assert_eq!(
            parse_key_value("filter=a=b"),
            Ok(("filter".to_owned(), "a=b".to_owned()))
        );
    }
}
