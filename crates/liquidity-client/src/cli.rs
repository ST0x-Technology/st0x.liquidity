//! Command-line model for the liquidity client: the argument parser, the
//! command and resource enums, and their fixed API path mappings.

use clap::{Args, Parser, Subcommand, ValueEnum};
use uuid::Uuid;

use crate::target::Env;
use crate::wire::{DecimalAmount, EvmAddress, ReconcileUsdcReason, TransferUsdcDirection, VaultId};

/// Rejects an empty or whitespace-only audit `--reason` at parse time, before
/// the value can reach auth or the network. The server rejects blank reasons
/// too, so this only moves the rejection earlier, into a clear clap error.
fn nonblank_reason(value: &str) -> Result<String, String> {
    if value.trim().is_empty() {
        Err("must not be blank".to_owned())
    } else {
        Ok(value.to_owned())
    }
}

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
    /// Capital movement operations, signed by the bot's own wallets (write
    /// tier, the same IAP group as debug).
    #[command(subcommand)]
    Capital(Capital),
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
        /// Transfer kind to recheck.
        #[arg(value_enum)]
        kind: RecheckTransferType,
        /// Aggregate id path segment.
        id: String,
        /// USDC only: the deposit tx to attach, for a BaseToAlpaca
        /// `DepositFailed` with no recorded deposit ref. Sent as the
        /// `deposit_tx` query parameter; the bot refuses it for mint and
        /// redemption.
        #[arg(long)]
        deposit_tx: Option<String>,
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
        /// The tx mined at a signed deposit send's nonce, for a BaseToAlpaca
        /// `Bridged` transfer whose send was cancelled; the bot verifies it on
        /// chain before reconciling.
        #[arg(long)]
        superseding_tx: Option<String>,
    },
    /// Reconcile a failed equity mint or redemption to OperatorReconciled.
    ReconcileEquity {
        /// Transfer kind.
        #[arg(value_enum)]
        kind: EquityTransferKind,
        /// Aggregate id.
        id: String,
        /// Free text audit reason, persisted on the event.
        #[arg(long, value_parser = nonblank_reason)]
        reason: String,
        /// The tx mined at a signed vault withdrawal's nonce, for a
        /// redemption whose withdrawal was cancelled: a reverted tx, or a
        /// 0-value self-transfer with no calldata (not EIP-7702) from the bot
        /// wallet. The bot verifies it on chain before reconciling.
        #[arg(long)]
        superseding_tx: Option<String>,
    },
    /// Adopt a mined tx that took a stuck redemption's signed vault withdrawal
    /// nonce and did the withdrawal itself (e.g. a wallet "speed up" of the
    /// same withdraw4), so the redemption finishes. The bot verifies it on
    /// chain before adopting it.
    AdoptWithdrawal {
        /// Redemption aggregate id.
        id: String,
        /// The mined tx at the withdrawal's nonce to adopt.
        #[arg(long)]
        replacement_tx: String,
        /// Free text audit reason, persisted on the event.
        #[arg(long, value_parser = nonblank_reason)]
        reason: String,
    },
    /// Clear a dropped CCTP burn hash from a USDC rebalance so the guard can
    /// be released.
    ClearPendingBurn {
        /// USDC rebalance id.
        id: String,
        /// Audit reason, logged with the action.
        #[arg(long, value_parser = nonblank_reason)]
        reason: String,
    },
    /// Mark an AlpacaToBase USDC rebalance failed before its burn; the guard
    /// stays held until reconcile-usdc settles the withdrawn funds. Refuses
    /// states after the burn and BaseToAlpaca transfers; verify onchain that
    /// no burn landed first.
    FailUsdcTransfer {
        /// USDC rebalance id.
        id: String,
        /// Free text audit reason, persisted on the event.
        #[arg(long, value_parser = nonblank_reason)]
        reason: String,
    },
    /// Force fail a stuck equity mint or redemption so its transfer guard
    /// can be released.
    FailEquityTransfer {
        /// Transfer kind.
        #[arg(value_enum)]
        kind: EquityTransferKind,
        /// Aggregate id.
        id: String,
        /// Free text audit reason, persisted on the event.
        #[arg(long, value_parser = nonblank_reason)]
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
        /// Hedged chain the fill happened on; omitted means the bot's primary
        /// chain.
        #[arg(long, value_enum)]
        chain: Option<HedgedChain>,
    },
    /// Rebuild a materialized view by replaying events.
    #[command(subcommand)]
    View(View),
    /// Recover stuck cross-chain USDC transfers.
    #[command(subcommand)]
    Cctp(Cctp),
}

/// A hedged chain, spelled as the bot's `chain` wire value (the `Chain` wire
/// names in `st0x-evm`): the `process-tx` query and the capital request bodies.
#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum HedgedChain {
    Base,
    Ethereum,
    Hyperevm,
    Robinhood,
}

impl HedgedChain {
    pub(crate) fn wire_name(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Ethereum => "ethereum",
            Self::Hyperevm => "hyperevm",
            Self::Robinhood => "robinhood",
        }
    }
}

/// USDC rebalance direction, spelled as the bot's path segment.
#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum UsdcDirection {
    AlpacaToBase,
    BaseToAlpaca,
}

#[derive(Subcommand)]
pub(crate) enum View {
    /// Rebuild a view or read model from scratch; the escape hatch for a view
    /// corrupted by a lost update.
    Rebuild(RebuildViewArgs),
}

#[derive(Args)]
pub(crate) struct RebuildViewArgs {
    /// View to rebuild.
    #[arg(value_enum)]
    pub(crate) view: RebuildableView,
    /// One aggregate's view (for example AAPL for position). The read models
    /// replay the whole model and take --all only.
    #[arg(long, conflicts_with = "all", required_unless_present = "all")]
    pub(crate) id: Option<String>,
    /// Every row of the view.
    #[arg(long, conflicts_with = "id", required_unless_present = "id")]
    pub(crate) all: bool,
}

/// The bot's rebuildable views, spelled as the route's path segment.
#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum RebuildableView {
    Position,
    OffchainOrder,
    VaultRegistry,
    RebalanceTiming,
    EquityTiming,
    LifecycleFailure,
    PortfolioSnapshot,
}

#[derive(Subcommand)]
pub(crate) enum Cctp {
    /// Complete the destination mint of a CCTP burn whose mint never landed.
    /// A burn Circle has not attested yet fails at once as retryable; rerun it
    /// later. Rerunning is always safe: a mint that already landed is adopted.
    /// Afterwards bring the stuck rebalance back in sync with resume-usdc or
    /// reconcile-usdc.
    CompleteMint {
        /// Transaction hash of the burn on the source chain.
        #[arg(long)]
        burn_tx: String,
        /// Chain the burn happened on; the mint lands on the other one.
        #[arg(long, value_enum)]
        source_chain: CctpSourceChain,
    },
}

/// The burn's chain, spelled as the bot's wire value.
#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum CctpSourceChain {
    Ethereum,
    Base,
}

impl CctpSourceChain {
    pub(crate) fn wire_name(self) -> &'static str {
        match self {
            Self::Ethereum => "ethereum",
            Self::Base => "base",
        }
    }
}

/// Equity transfer kind, spelled as the bot's reconcile path segment.
#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum EquityTransferKind {
    Mint,
    Redemption,
}

impl EquityTransferKind {
    /// The `{kind}` segment of the bot's equity transfer routes.
    pub(crate) const fn route_segment(self) -> &'static str {
        match self {
            Self::Mint => "equity_mint",
            Self::Redemption => "equity_redemption",
        }
    }
}

/// Recheck transfer kind. A superset of `EquityTransferKind`: a failed USDC
/// deposit leg is recheckable too. Each variant maps to the bot's recheck path
/// segment in `main.rs`.
#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum RecheckTransferType {
    Mint,
    Redemption,
    Usdc,
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
    #[arg(long, value_parser = nonblank_reason)]
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
    #[arg(long, value_parser = nonblank_reason)]
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
    #[arg(long, value_parser = nonblank_reason)]
    pub(crate) reason: String,
}

#[derive(Subcommand)]
pub(crate) enum Capital {
    /// Start a USDC transfer between Alpaca and Raindex on the bot's transfer
    /// worker, the path the rebalancer uses. Returns the new transfer id at
    /// once; follow it with `read` and recover it with `debug resume-usdc`.
    TransferUsdc {
        /// Direction of transfer.
        #[arg(short = 'd', long, value_enum)]
        direction: TransferUsdcDirection,
        /// Amount of USDC to transfer, as a decimal.
        #[arg(short = 'a', long, allow_negative_numbers = true)]
        amount: DecimalAmount,
        /// Chain of the served cash corridor to run on; may be left out only
        /// while the bot serves one corridor.
        #[arg(long, value_enum)]
        chain: Option<HedgedChain>,
    },
    /// Deposit tokens from the bot's wallet into a Raindex vault: approves,
    /// then deposits, resolving token decimals from onchain metadata. Returns
    /// the deposit tx as soon as it is broadcast; the bot confirms it
    /// afterwards and logs the outcome. When the allowance is short it first
    /// waits for the approve to confirm, so a token without the startup MAX
    /// grant can still time out on a chain that needs many confirmations.
    /// Once it answers with a tx, a rerun answers 409 until that tx's outcome
    /// is logged. A 500 leaves no lock and its tx may still have gone out, so
    /// check the vault onchain before rerunning.
    VaultDeposit(VaultArgs),
    /// Withdraw tokens from a Raindex vault to the bot's wallet, resolving
    /// token decimals from onchain metadata. Returns the withdraw tx as soon
    /// as it is broadcast; the bot confirms it afterwards and logs the
    /// outcome. Once it answers with a tx, a rerun, or a `vault-withdraw-usdc`,
    /// answers 409 until that tx's outcome is logged. A 500 leaves no lock and
    /// its tx may still have gone out, so check the vault onchain first.
    VaultWithdraw(VaultArgs),
    /// Withdraw the chain's settlement stable (USDC, or USDG on Robinhood)
    /// from its configured Raindex cash vault. Returns the withdraw tx as
    /// soon as it is broadcast; the bot confirms it afterwards and logs the
    /// outcome. Once it answers with a tx, a rerun, or a `vault-withdraw`,
    /// answers 409 until that tx's outcome is logged. A 500 leaves no lock and
    /// its tx may still have gone out, so check the vault onchain first.
    VaultWithdrawUsdc {
        /// Amount of the settlement stable to withdraw, as a decimal.
        #[arg(short = 'a', long, allow_negative_numbers = true)]
        amount: DecimalAmount,
        /// Chain of the cash vault: its settlement stable and its first
        /// `[chains.<name>.trading.assets.cash]` vault; a chain with no cash
        /// vault configured is refused.
        #[arg(long, value_enum, default_value_t = HedgedChain::Base)]
        network: HedgedChain,
    },
    /// Burn USDC on Ethereum or Base for a CCTP transfer to the other chain.
    /// Returns the burn tx and its status as soon as the burn is broadcast,
    /// without waiting for its receipt or Circle's attestation; finish with
    /// `debug cctp complete-mint` once it is attested. Every run sends an
    /// operation id, printed to stderr before the request: rerunning with
    /// `--operation-id <id>` after a failure or a timeout reports that same
    /// burn and its status instead of burning again.
    CctpBridge {
        /// Amount of USDC to bridge, as a decimal (omit to use --all).
        #[arg(
            short = 'a',
            long,
            allow_negative_numbers = true,
            conflicts_with = "all",
            required_unless_present = "all"
        )]
        amount: Option<DecimalAmount>,
        /// Bridge the entire USDC balance of the source wallet.
        #[arg(long, conflicts_with = "amount", required_unless_present = "amount")]
        all: bool,
        /// Source chain to burn from; the mint lands on the other one.
        #[arg(long, value_enum)]
        from: CctpSourceChain,
        /// The operation id an earlier run printed, to report its burn
        /// instead of burning again. Omit for a new burn: a fresh id is
        /// generated. A rerun must repeat the same `--from` and amount.
        #[arg(long)]
        operation_id: Option<Uuid>,
    },
    /// Settle a pending `cctp-bridge` burn that can never mine because
    /// another tx from the source wallet took its nonce: first cancel it with
    /// a 0 value transfer to the wallet itself, with no calldata, at the
    /// burn's nonce and a higher fee, then pass that tx once it has the
    /// chain's required confirmations. Startup then stops rebroadcasting the
    /// burn. A burn whose own receipt already decides reports that status.
    CctpBurnSupersede {
        /// The operation id the burn's `cctp-bridge` run printed.
        #[arg(long)]
        operation_id: Uuid,
        /// The mined tx that took the burn's nonce.
        #[arg(long)]
        superseding_tx: String,
    },
    /// Reset the bot wallet's settlement stable allowance (USDC, or USDG on
    /// Robinhood) for the orderbook to zero. Returns the revoke tx as soon as
    /// it is broadcast; the bot confirms it afterwards and logs the outcome. A
    /// rerun before the confirmation sends a redundant revoke.
    ResetAllowance {
        /// Chain whose allowance to reset: its wallet, its settlement stable
        /// and its `[chains.<name>.trading]` orderbook.
        #[arg(long, value_enum, default_value_t = HedgedChain::Base)]
        network: HedgedChain,
    },
}

/// Shared by `vault-deposit` and `vault-withdraw`, whose requests carry the
/// same body.
#[derive(Args, Clone)]
pub(crate) struct VaultArgs {
    /// Amount of tokens as a decimal (for example 100 for 100 tokens).
    #[arg(short = 'a', long, allow_negative_numbers = true)]
    pub(crate) amount: DecimalAmount,
    /// Token contract address.
    #[arg(short = 't', long)]
    pub(crate) token: EvmAddress,
    /// Vault ID.
    #[arg(short = 'v', long)]
    pub(crate) vault_id: VaultId,
    /// Chain of the vault: selects the wallet and the
    /// `[chains.<name>.trading]` orderbook; a chain without that table is
    /// refused.
    #[arg(long, value_enum, default_value_t = HedgedChain::Base)]
    pub(crate) network: HedgedChain,
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
        Capital, CctpSourceChain, Cli, Command, Debug, EquityTransferKind, PortfolioSnapshot,
        Position, UsdcDirection, parse_key_value,
    };
    use crate::target::Env;
    use crate::wire::{
        DecimalAmount, EvmAddress, ReconcileUsdcReason, TransferUsdcDirection, VaultId,
    };

    const TOKEN: &str = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913";
    const VAULT: &str = "0x00000000000000000000000000000000000000000000000000000000000000a1";

    fn amount(raw: &str) -> DecimalAmount {
        raw.parse().unwrap()
    }

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
            Command::Read(_) | Command::Capital(_) => panic!("expected a debug command"),
        })
    }

    /// Every free-text `--reason` shares the nonblank parser, so an empty or
    /// whitespace-only value is refused by clap before auth or the network,
    /// rather than round-tripping to the server's blank check.
    #[test]
    fn blank_reasons_are_rejected_at_parse_time() {
        let blanks = ["", "   ", "\t", "\n"];
        let verbs: &[&[&str]] = &[
            &["reconcile-equity", "mint", "abc", "--reason"],
            &[
                "adopt-withdrawal",
                "abc",
                "--replacement-tx",
                "0xa",
                "--reason",
            ],
            &["clear-pending-burn", "abc", "--reason"],
            &["fail-usdc-transfer", "abc", "--reason"],
            &["fail-equity-transfer", "redemption", "abc", "--reason"],
            &["position", "set", "AAPL", "--target-net", "1", "--reason"],
            &[
                "position",
                "release-hedge",
                "AAPL",
                "--order-id",
                "ord-1",
                "--reason",
            ],
            &[
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
                "nasdaq",
                "--reason",
            ],
        ];
        for verb in verbs {
            for blank in blanks {
                let argv: Vec<&str> = verb.iter().copied().chain([blank]).collect();
                let Err(error) = debug(&argv) else {
                    panic!("{argv:?} must be rejected");
                };
                assert_eq!(
                    error.kind(),
                    clap::error::ErrorKind::ValueValidation,
                    "{argv:?}"
                );
            }
            // A non-blank reason with the same shape parses.
            let argv: Vec<&str> = verb.iter().copied().chain(["real reason"]).collect();
            debug(&argv).unwrap_or_else(|error| panic!("{argv:?} must parse: {error}"));
        }
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
                superseding_tx: None,
            } if id == "abc"
        ));
        assert!(matches!(
            debug(&[
                "reconcile-usdc",
                "abc",
                "--reason",
                "funds-moved-manually",
                "--superseding-tx",
                "0xcancel"
            ])
            .unwrap(),
            Debug::ReconcileUsdc {
                superseding_tx: Some(tx),
                ..
            } if tx == "0xcancel"
        ));
        assert!(matches!(
            debug(&["reconcile-equity", "redemption", "abc", "--reason", "settled"]).unwrap(),
            Debug::ReconcileEquity {
                kind: EquityTransferKind::Redemption,
                id,
                reason,
                superseding_tx: None,
            } if id == "abc" && reason == "settled"
        ));
        assert!(matches!(
            debug(&[
                "reconcile-equity",
                "redemption",
                "abc",
                "--reason",
                "settled",
                "--superseding-tx",
                "0xcancel",
            ])
            .unwrap(),
            Debug::ReconcileEquity {
                superseding_tx: Some(tx),
                ..
            } if tx == "0xcancel"
        ));
        assert!(matches!(
            debug(&["clear-pending-burn", "abc", "--reason", "dropped"]).unwrap(),
            Debug::ClearPendingBurn { id, reason } if id == "abc" && reason == "dropped"
        ));
        assert!(matches!(
            debug(&["fail-usdc-transfer", "abc", "--reason", "pre-burn crash"]).unwrap(),
            Debug::FailUsdcTransfer { id, reason } if id == "abc" && reason == "pre-burn crash"
        ));
        assert!(matches!(
            debug(&["fail-equity-transfer", "mint", "abc", "--reason", "stuck"]).unwrap(),
            Debug::FailEquityTransfer {
                kind: EquityTransferKind::Mint,
                id,
                reason,
            } if id == "abc" && reason == "stuck"
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
            Debug::ProcessTx { tx_hash, chain: None } if tx_hash == "0xabc"
        ));
        for (spelling, expected) in [
            ("base", "base"),
            ("ethereum", "ethereum"),
            ("hyperevm", "hyperevm"),
            ("robinhood", "robinhood"),
        ] {
            let Debug::ProcessTx {
                chain: Some(chain), ..
            } = debug(&["process-tx", "0xabc", "--chain", spelling]).unwrap()
            else {
                panic!("--chain {spelling} must parse");
            };
            assert_eq!(chain.wire_name(), expected);
        }
        let Err(error) = debug(&["process-tx", "0xabc", "--chain", "solana"]) else {
            panic!("--chain solana must be refused");
        };
        assert_eq!(error.kind(), clap::error::ErrorKind::InvalidValue);
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

    /// Parses a full argv (after `--env staging`) into the capital command.
    fn capital(args: &[&str]) -> Result<Capital, clap::Error> {
        let full = ["st0x-liquidity-client", "--env", "staging", "capital"]
            .into_iter()
            .chain(args.iter().copied());
        Cli::try_parse_from(full).map(|cli| match cli.command {
            Command::Capital(capital) => capital,
            Command::Read(_) | Command::Debug(_) => panic!("expected a capital command"),
        })
    }

    /// The capital verbs keep the flag names and short flags of their
    /// st0x-cli counterparts, and pass amounts through verbatim.
    #[test]
    fn parses_capital_verbs() {
        assert!(matches!(
            capital(&[
                "transfer-usdc",
                "--direction",
                "to-raindex",
                "--amount",
                "250.5"
            ])
            .unwrap(),
            Capital::TransferUsdc {
                direction: TransferUsdcDirection::ToRaindex,
                amount,
                chain: None,
            } if amount == self::amount("250.5")
        ));
        assert!(matches!(
            capital(&["transfer-usdc", "-d", "to-alpaca", "-a", "10", "--chain", "robinhood"])
                .unwrap(),
            Capital::TransferUsdc {
                direction: TransferUsdcDirection::ToAlpaca,
                amount,
                chain: Some(super::HedgedChain::Robinhood),
            } if amount == self::amount("10")
        ));

        let Capital::VaultDeposit(deposit) = capital(&[
            "vault-deposit",
            "-a",
            "1.5",
            "-t",
            TOKEN,
            "-v",
            VAULT,
            "--network",
            "ethereum",
        ])
        .unwrap() else {
            panic!("expected vault-deposit");
        };
        assert_eq!(deposit.amount, amount("1.5"));
        assert_eq!(deposit.token, TOKEN.parse::<EvmAddress>().unwrap());
        assert_eq!(deposit.vault_id, VAULT.parse::<VaultId>().unwrap());
        assert_eq!(deposit.network.wire_name(), "ethereum");

        let Capital::VaultWithdraw(withdraw) = capital(&[
            "vault-withdraw",
            "--amount",
            "2",
            "--token",
            TOKEN,
            "--vault-id",
            VAULT,
            "--network",
            "robinhood",
        ])
        .unwrap() else {
            panic!("expected vault-withdraw");
        };
        assert_eq!(withdraw.amount, amount("2"));
        assert_eq!(withdraw.vault_id, VAULT.parse::<VaultId>().unwrap());
        assert_eq!(withdraw.network.wire_name(), "robinhood");

        let Capital::VaultWithdrawUsdc {
            amount: withdrawn,
            network,
        } = capital(&["vault-withdraw-usdc", "-a", "100", "--network", "hyperevm"]).unwrap()
        else {
            panic!("expected vault-withdraw-usdc");
        };
        assert_eq!(withdrawn, amount("100"));
        assert_eq!(network.wire_name(), "hyperevm");

        let Capital::ResetAllowance { network } =
            capital(&["reset-allowance", "--network", "ethereum"]).unwrap()
        else {
            panic!("expected reset-allowance");
        };
        assert_eq!(network.wire_name(), "ethereum");

        for argv in [
            &["transfer-usdc", "--direction", "to-raindex"][..],
            &["vault-deposit", "-a", "1", "-t", TOKEN][..],
            &["vault-withdraw", "-a", "1", "-v", VAULT][..],
            &["vault-withdraw-usdc", "--network", "base"][..],
            &["cctp-bridge", "--amount", "1"][..],
        ] {
            let Err(error) = capital(argv) else {
                panic!("{argv:?} must require its flags");
            };
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::MissingRequiredArgument,
                "{argv:?} must require its flags"
            );
        }
    }

    /// Like st0x-cli, every capital `--network` defaults to base.
    #[test]
    fn capital_network_defaults_to_base() {
        for argv in [
            &["vault-deposit", "-a", "1", "-t", TOKEN, "-v", VAULT][..],
            &["vault-withdraw", "-a", "1", "-t", TOKEN, "-v", VAULT][..],
            &["vault-withdraw-usdc", "-a", "1"][..],
            &["reset-allowance"][..],
        ] {
            let network = match capital(argv).unwrap() {
                Capital::VaultDeposit(args) | Capital::VaultWithdraw(args) => args.network,
                Capital::VaultWithdrawUsdc { network, .. }
                | Capital::ResetAllowance { network } => network,
                Capital::TransferUsdc { .. }
                | Capital::CctpBridge { .. }
                | Capital::CctpBurnSupersede { .. } => {
                    panic!("{argv:?} takes no --network")
                }
            };
            assert_eq!(network.wire_name(), "base", "{argv:?}");
        }
    }

    /// `cctp-bridge` takes exactly one of `--amount` and `--all`, refused by
    /// clap before the bot's own check, and an optional `--operation-id`.
    #[test]
    fn cctp_bridge_requires_exactly_one_of_amount_and_all() {
        assert!(matches!(
            capital(&["cctp-bridge", "--from", "ethereum", "--amount", "100"]).unwrap(),
            Capital::CctpBridge {
                amount: Some(amount),
                all: false,
                from: CctpSourceChain::Ethereum,
                operation_id: None,
            } if amount == self::amount("100")
        ));
        let id = "6f1c2a1e-6c39-4a77-9a8e-1f0b7d6e8c11";
        assert!(matches!(
            capital(&["cctp-bridge", "--from", "base", "--all", "--operation-id", id]).unwrap(),
            Capital::CctpBridge {
                amount: None,
                all: true,
                from: CctpSourceChain::Base,
                operation_id: Some(operation_id),
            } if operation_id.to_string() == id
        ));

        let Err(error) = capital(&["cctp-bridge", "--from", "base", "--amount", "1", "--all"])
        else {
            panic!("--amount with --all must be refused");
        };
        assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);

        let Err(error) = capital(&["cctp-bridge", "--from", "base"]) else {
            panic!("neither --amount nor --all must be refused");
        };
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    /// Capital value enums refuse unknown spellings before any network call,
    /// and `reset-allowance` takes `--network`, not `--chain`, like st0x-cli.
    #[test]
    fn capital_value_enums_reject_unknown_spellings() {
        for argv in [
            &["transfer-usdc", "--direction", "sideways", "--amount", "1"][..],
            &[
                "vault-deposit",
                "-a",
                "1",
                "-t",
                TOKEN,
                "-v",
                VAULT,
                "--network",
                "solana",
            ][..],
            &["vault-withdraw-usdc", "-a", "1", "--network", "solana"][..],
            &["reset-allowance", "--network", "solana"][..],
            &["cctp-bridge", "--from", "hyperevm", "--all"][..],
        ] {
            let Err(error) = capital(argv) else {
                panic!("{argv:?} must be refused");
            };
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::InvalidValue,
                "{argv:?} must be refused"
            );
        }

        let Err(error) = capital(&["reset-allowance", "--chain", "base"]) else {
            panic!("--chain must be refused");
        };
        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    /// Malformed amounts, token addresses and vault ids are refused by clap
    /// before any authentication or bot round trip.
    #[test]
    fn capital_refuses_malformed_amount_token_and_vault_id() {
        let mut argvs: Vec<Vec<&str>> = Vec::new();
        for bad_amount in ["0", "-1", "abc", "1e3", "0.00", "1.", ".5"] {
            argvs.push(vec!["transfer-usdc", "-d", "to-alpaca", "-a", bad_amount]);
            argvs.push(vec![
                "vault-deposit",
                "-a",
                bad_amount,
                "-t",
                TOKEN,
                "-v",
                VAULT,
            ]);
            argvs.push(vec!["vault-withdraw-usdc", "-a", bad_amount]);
            argvs.push(vec![
                "cctp-bridge",
                "--from",
                "base",
                "--amount",
                bad_amount,
            ]);
        }
        for bad_token in [
            "0xtoken",
            "833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "0x1234",
        ] {
            argvs.push(vec![
                "vault-deposit",
                "-a",
                "1",
                "-t",
                bad_token,
                "-v",
                VAULT,
            ]);
        }
        for bad_vault in ["0xvault", TOKEN, "1"] {
            argvs.push(vec![
                "vault-withdraw",
                "-a",
                "1",
                "-t",
                TOKEN,
                "--vault-id",
                bad_vault,
            ]);
        }
        for argv in argvs {
            let Err(error) = capital(&argv) else {
                panic!("{argv:?} must be refused");
            };
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::ValueValidation,
                "{argv:?} must be refused"
            );
        }
    }

    /// Valid values parse and serialize to exactly the spelling typed.
    #[test]
    fn capital_values_serialize_as_typed() {
        let Capital::VaultDeposit(deposit) =
            capital(&["vault-deposit", "-a", "0.000001", "-t", TOKEN, "-v", VAULT]).unwrap()
        else {
            panic!("expected vault-deposit");
        };
        assert_eq!(
            serde_json::to_value(&deposit.amount).unwrap(),
            serde_json::json!("0.000001")
        );
        assert_eq!(
            serde_json::to_value(&deposit.token).unwrap(),
            serde_json::json!(TOKEN)
        );
        assert_eq!(
            serde_json::to_value(&deposit.vault_id).unwrap(),
            serde_json::json!(VAULT)
        );
    }

    #[test]
    fn decimal_amount_boundaries() {
        for good in ["100", "1.5", "0.000001", "0010", "10.0"] {
            assert_eq!(
                serde_json::to_value(amount(good)).unwrap(),
                serde_json::json!(good)
            );
        }
        for bad in [
            "", "0", "0.00", "00", "-1", "+1", "1e3", " 1", "1 ", "1.", ".5", "1.2.3", "abc", "1,5",
        ] {
            let Err(message) = bad.parse::<DecimalAmount>() else {
                panic!("{bad:?} must be refused");
            };
            assert!(message.contains(&format!("`{bad}`")), "{message}");
        }
    }

    #[test]
    fn evm_address_boundaries() {
        let hex40 = "a".repeat(40);
        for good in [format!("0x{hex40}"), TOKEN.to_owned()] {
            assert_eq!(
                serde_json::to_value(good.parse::<EvmAddress>().unwrap()).unwrap(),
                serde_json::json!(good)
            );
        }
        for bad in [
            format!("0x{}", "a".repeat(39)),
            format!("0x{}", "a".repeat(41)),
            hex40.clone(),
            format!("0X{hex40}"),
            format!("0x{}g", "a".repeat(39)),
            format!("0x{hex40} "),
        ] {
            let Err(message) = bad.parse::<EvmAddress>() else {
                panic!("{bad:?} must be refused");
            };
            assert!(message.contains("EVM address"), "{message}");
        }
    }

    #[test]
    fn vault_id_boundaries() {
        let hex64 = "F".repeat(64);
        for good in [format!("0x{hex64}"), VAULT.to_owned()] {
            assert_eq!(
                serde_json::to_value(good.parse::<VaultId>().unwrap()).unwrap(),
                serde_json::json!(good)
            );
        }
        for bad in [
            format!("0x{}", "f".repeat(63)),
            format!("0x{}", "f".repeat(65)),
            format!("0x{}z", "f".repeat(63)),
            hex64,
        ] {
            let Err(message) = bad.parse::<VaultId>() else {
                panic!("{bad:?} must be refused");
            };
            assert!(message.contains("vault id"), "{message}");
        }
    }
}
