//! Command-line model for the liquidity client: the argument parser, the
//! command and resource enums, and their fixed API path mappings.

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::target::Env;
use crate::wire::ReconcileUsdcReason;

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
    /// Enqueue a manual resume of one USDC rebalance on the bot's transfer
    /// worker.
    ResumeUsdc {
        /// Rebalance direction.
        #[arg(value_enum)]
        direction: UsdcDirection,
        /// USDC rebalance id.
        id: String,
    },
    /// Reconcile a stuck USDC rebalance to OperatorReconciled.
    ReconcileUsdc {
        /// USDC rebalance id.
        id: String,
        /// Audit reason, from the fixed vocabulary the bot accepts.
        #[arg(long, value_enum)]
        reason: ReconcileUsdcReason,
    },
    /// Reconcile a failed equity mint or redemption to OperatorReconciled.
    ReconcileEquity {
        /// Transfer kind.
        #[arg(value_enum)]
        kind: EquityTransferKind,
        /// Aggregate id.
        id: String,
        /// Free text audit reason, persisted on the event.
        #[arg(long)]
        reason: String,
    },
    /// Clear a dropped CCTP burn hash from a USDC rebalance so the guard can
    /// be released.
    ClearPendingBurn {
        /// USDC rebalance id.
        id: String,
        /// Audit reason, logged with the action.
        #[arg(long)]
        reason: String,
    },
    /// Mark a pre-burn USDC rebalance failed so the guard can be released.
    /// Refuses post-burn states; verify on-chain that no burn landed first.
    FailUsdcTransfer {
        /// USDC rebalance id.
        id: String,
        /// Free text audit reason, persisted on the event.
        #[arg(long)]
        reason: String,
    },
    /// Recover stuck position state.
    #[command(subcommand)]
    Position(Position),
    /// Repair daily portfolio marks.
    #[command(subcommand)]
    PortfolioSnapshot(PortfolioSnapshot),
    /// Account a missed onchain fill and place the opposite hedge.
    ProcessTx {
        /// Transaction hash of the onchain fill.
        tx_hash: String,
    },
}

/// USDC rebalance direction, spelled as the bot's path segment.
#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum UsdcDirection {
    AlpacaToBase,
    BaseToAlpaca,
}

/// Equity transfer kind, spelled as the bot's reconcile path segment.
#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum EquityTransferKind {
    Mint,
    Redemption,
}

#[derive(Subcommand)]
pub(crate) enum Position {
    /// Set a position's net exposure after a manual correction.
    Set(SetPositionArgs),
    /// Fail a position's pending hedge order and clear its pending pointer.
    ReleaseHedge(ReleaseHedgeArgs),
}

#[derive(Args)]
pub(crate) struct SetPositionArgs {
    /// Equity symbol.
    pub(crate) symbol: String,
    /// Signed decimal net exposure to set (negative is short).
    #[arg(long, allow_negative_numbers = true)]
    pub(crate) target_net: String,
    /// USDC price per share; required for nonzero targets under a dollar
    /// value threshold.
    #[arg(long)]
    pub(crate) price_usdc: Option<String>,
    /// Free text audit reason, persisted on the event.
    #[arg(long)]
    pub(crate) reason: String,
}

#[derive(Args)]
pub(crate) struct ReleaseHedgeArgs {
    /// Equity symbol.
    pub(crate) symbol: String,
    /// Pending offchain order id recorded on the position.
    #[arg(long)]
    pub(crate) order_id: String,
    /// Free text audit reason, persisted on the event.
    #[arg(long)]
    pub(crate) reason: String,
}

#[derive(Subcommand)]
pub(crate) enum PortfolioSnapshot {
    /// Set the audited historical closing price mark for one captured ET day.
    SetMark(SetMarkArgs),
}

#[derive(Args)]
pub(crate) struct SetMarkArgs {
    /// ET day of the captured balance snapshot (YYYY-MM-DD).
    #[arg(long)]
    pub(crate) day: String,
    /// Equity symbol whose mark applies at every captured location.
    #[arg(long)]
    pub(crate) symbol: String,
    /// Strictly positive historical USD closing price per share.
    #[arg(long)]
    pub(crate) usd_mark: String,
    /// Sourced economic timestamp (RFC 3339); an earlier ET day.
    #[arg(long)]
    pub(crate) observed_at: String,
    /// Source used to verify the historical price.
    #[arg(long)]
    pub(crate) source: String,
    /// Free text audit reason, persisted on the event.
    #[arg(long)]
    pub(crate) reason: String,
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

    use super::{
        Cli, Command, Debug, EquityTransferKind, PortfolioSnapshot, Position, UsdcDirection,
        parse_key_value,
    };
    use crate::target::Env;
    use crate::wire::ReconcileUsdcReason;

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

    /// Parses a full argv (after `--env staging`) into the debug command.
    fn debug(args: &[&str]) -> Result<Debug, clap::Error> {
        let full = ["st0x-liquidity-client", "--env", "staging", "debug"]
            .into_iter()
            .chain(args.iter().copied());
        Cli::try_parse_from(full).map(|cli| match cli.command {
            Command::Debug(debug) => debug,
            Command::Read(_) => panic!("expected a debug command"),
        })
    }

    /// The verbs whose only inputs are positionals and a `--reason`: the
    /// command name, the argument order, and the reason requirement are the
    /// operator-facing contract.
    #[test]
    fn parses_reason_bearing_debug_verbs() {
        assert!(matches!(
            debug(&["reconcile-usdc", "abc", "--reason", "funds-moved-manually"]).unwrap(),
            Debug::ReconcileUsdc {
                id,
                reason: ReconcileUsdcReason::FundsMovedManually,
            } if id == "abc"
        ));
        assert!(matches!(
            debug(&["reconcile-equity", "redemption", "abc", "--reason", "settled"]).unwrap(),
            Debug::ReconcileEquity {
                kind: EquityTransferKind::Redemption,
                id,
                reason,
            } if id == "abc" && reason == "settled"
        ));
        assert!(matches!(
            debug(&["clear-pending-burn", "abc", "--reason", "dropped"]).unwrap(),
            Debug::ClearPendingBurn { id, reason } if id == "abc" && reason == "dropped"
        ));
        assert!(matches!(
            debug(&["fail-usdc-transfer", "abc", "--reason", "pre-burn crash"]).unwrap(),
            Debug::FailUsdcTransfer { id, reason } if id == "abc" && reason == "pre-burn crash"
        ));

        for verb in ["reconcile-usdc", "clear-pending-burn", "fail-usdc-transfer"] {
            let Err(error) = debug(&[verb, "abc"]) else {
                panic!("{verb} must require --reason");
            };
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::MissingRequiredArgument,
                "{verb} must require --reason"
            );
        }
    }

    /// Value enums are validated by clap before any network call, with the
    /// kebab-case spellings the help text advertises.
    #[test]
    fn value_enums_accept_their_spellings_and_reject_others() {
        assert!(matches!(
            debug(&["resume-usdc", "alpaca-to-base", "abc"]).unwrap(),
            Debug::ResumeUsdc {
                direction: UsdcDirection::AlpacaToBase,
                ..
            }
        ));
        assert!(matches!(
            debug(&["resume-usdc", "base-to-alpaca", "abc"]).unwrap(),
            Debug::ResumeUsdc {
                direction: UsdcDirection::BaseToAlpaca,
                ..
            }
        ));
        assert!(matches!(
            debug(&["reconcile-equity", "mint", "abc", "--reason", "x"]).unwrap(),
            Debug::ReconcileEquity {
                kind: EquityTransferKind::Mint,
                ..
            }
        ));
        assert!(matches!(
            debug(&[
                "reconcile-usdc",
                "abc",
                "--reason",
                "deposit-credited-offline"
            ])
            .unwrap(),
            Debug::ReconcileUsdc {
                reason: ReconcileUsdcReason::DepositCreditedOffline,
                ..
            }
        ));

        for argv in [
            &["resume-usdc", "sideways", "abc"][..],
            &["reconcile-equity", "usdc", "abc", "--reason", "x"][..],
            &["reconcile-usdc", "abc", "--reason", "typo"][..],
        ] {
            let Err(error) = debug(argv) else {
                panic!("{argv:?} must be refused");
            };
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::InvalidValue,
                "{argv:?} must be refused"
            );
        }
    }

    #[test]
    fn parses_process_tx() {
        assert!(matches!(
            debug(&["process-tx", "0xabc"]).unwrap(),
            Debug::ProcessTx { tx_hash } if tx_hash == "0xabc"
        ));
    }

    /// The nested groups: every flag on `position set` / `release-hedge` and
    /// `portfolio-snapshot set-mark` is required except the optional price.
    #[test]
    fn parses_nested_position_and_snapshot_verbs() {
        let Debug::Position(Position::Set(set)) = debug(&[
            "position",
            "set",
            "AAPL",
            "--target-net",
            "-1.5",
            "--reason",
            "manual",
        ])
        .unwrap() else {
            panic!("expected position set");
        };
        assert_eq!(set.symbol, "AAPL");
        assert_eq!(set.target_net, "-1.5");
        assert_eq!(set.price_usdc, None);

        let Debug::Position(Position::Set(set)) = debug(&[
            "position",
            "set",
            "AAPL",
            "--target-net",
            "2",
            "--price-usdc",
            "150.25",
            "--reason",
            "manual",
        ])
        .unwrap() else {
            panic!("expected position set");
        };
        assert_eq!(set.price_usdc.as_deref(), Some("150.25"));

        let Debug::Position(Position::ReleaseHedge(release)) = debug(&[
            "position",
            "release-hedge",
            "AAPL",
            "--order-id",
            "ord-1",
            "--reason",
            "cancelled",
        ])
        .unwrap() else {
            panic!("expected position release-hedge");
        };
        assert_eq!(release.order_id, "ord-1");

        let Debug::PortfolioSnapshot(PortfolioSnapshot::SetMark(mark)) = debug(&[
            "portfolio-snapshot",
            "set-mark",
            "--day",
            "2026-09-01",
            "--symbol",
            "AAPL",
            "--usd-mark",
            "150.25",
            "--observed-at",
            "2026-09-01T20:00:00Z",
            "--source",
            "nasdaq",
            "--reason",
            "stale",
        ])
        .unwrap() else {
            panic!("expected portfolio-snapshot set-mark");
        };
        assert_eq!(mark.day, "2026-09-01");
        assert_eq!(mark.source, "nasdaq");

        for argv in [
            &["position", "set", "AAPL", "--reason", "manual"][..],
            &["position", "release-hedge", "AAPL", "--reason", "cancelled"][..],
            &["portfolio-snapshot", "set-mark", "--day", "2026-09-01"][..],
        ] {
            let Err(error) = debug(argv) else {
                panic!("{argv:?} must require its flags");
            };
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::MissingRequiredArgument,
                "{argv:?} must require its flags"
            );
        }
    }
}
