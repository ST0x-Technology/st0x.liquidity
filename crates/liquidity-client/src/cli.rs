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
    Read(ReadArgs),
}

#[derive(Args)]
pub struct ReadArgs {
    /// Resource to read.
    #[arg(value_enum)]
    pub resource: ReadResource,
    /// Extra query parameter, repeatable: --param key=value
    #[arg(long = "param", value_parser = parse_key_value)]
    pub params: Vec<(String, String)>,
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
