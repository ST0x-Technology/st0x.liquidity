//! CLI commands for trading, asset transfers, and authentication.

mod alpaca_wallet;
mod cctp;
mod dividend;
mod rebalancing;
mod repair;
mod submit;
mod trading;
mod vault;
mod wrapper;

use alloy::primitives::{Address, B256, TxHash};
use alloy::providers::ProviderBuilder;
use clap::{ArgGroup, Parser, Subcommand, ValueEnum};
use rain_math_float::Float;
use sqlx::SqlitePool;
use std::io::Write;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use st0x_config::{Ctx, Env};
use st0x_event_sorcery::Projection;
use st0x_evm::OpenChainErrorRegistry;
use st0x_execution::alpaca_broker_api::AlpacaLimitPrice;
use st0x_execution::{AlpacaAccountId, Direction, FractionalShares, Positive, Symbol, TimeInForce};
use st0x_finance::Usdc;

use crate::offchain::order::{OffchainOrder, OffchainOrderId, OrderPlacer};
use crate::performance::rebalance::RebalanceTimingProjection;
use crate::performance::reliability::LifecycleFailureProjection;
use crate::position::Position;
use crate::symbol::cache::SymbolCache;
use crate::vault_registry::VaultRegistry;

/// Direction for transferring assets between trading venues.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TransferDirection {
    /// Transfer to Raindex (onchain venue)
    ToRaindex,
    /// Transfer to Alpaca (offchain venue)
    ToAlpaca,
}

/// Direction for USDC/USD conversion on Alpaca.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ConvertDirection {
    /// Convert USDC to USD buying power (sell USDC/USD)
    ToUsd,
    /// Convert USD buying power to USDC (buy USDC/USD)
    ToUsdc,
}

/// Transfer type for the `transfer fail` command.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TransferType {
    /// Tokenized equity mint (Alpaca -> onchain)
    Mint,
    /// Equity redemption (onchain -> Alpaca)
    Redemption,
}

/// Why an operator is reconciling a stuck post-burn USDC transfer.
///
/// CLI-facing mirror of [`crate::usdc_rebalance::ReconcileReason`]; mapped to
/// the domain enum in the command handler so clap stays out of the aggregate.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ReconcileReasonArg {
    /// The minted USDC was moved to its destination manually.
    FundsMovedManually,
    /// The deposit was credited at the destination outside the bot's view.
    DepositCreditedOffline,
}

impl From<ReconcileReasonArg> for crate::usdc_rebalance::ReconcileReason {
    fn from(reason: ReconcileReasonArg) -> Self {
        match reason {
            ReconcileReasonArg::FundsMovedManually => Self::FundsMovedManually,
            ReconcileReasonArg::DepositCreditedOffline => Self::DepositCreditedOffline,
        }
    }
}

/// What kind of transfer to resume.
///
/// `usdc` resumes a single USDC rebalance by id, directly against the local CQRS
/// state. `equity` resumes ALL interrupted mints and redemptions via the running
/// bot's REST endpoint (always ALL interrupted transfers; no per-id filter).
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TransferResumeKind {
    Usdc,
    Equity,
}

/// What kind of transfer to reconcile.
///
/// `usdc` reconciles a post-burn `DepositFailed` USDC rebalance; `mint` and
/// `redemption` reconcile an equity transfer stuck in `Failed`. All operate
/// directly on the local CQRS state via aggregate commands.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ReconcileKind {
    Usdc,
    Mint,
    Redemption,
}

/// Read models rebuildable by replaying events from scratch.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum AggregateView {
    /// Position aggregate (position_view)
    Position,
    /// Offchain order aggregate (offchain_order_view)
    OffchainOrder,
    /// Vault registry aggregate (vault_registry_view)
    VaultRegistry,
    /// Rebalance stage-timing read model (rebalance_stage_timing). Replays every
    /// `UsdcRebalance` event stream through the reactor fold. Supports `--all` only.
    RebalanceTiming,
    /// Lifecycle-failure read model (lifecycle_failure_event). Replays every
    /// failure across all four subscribed streams through the reactor fold.
    /// Supports `--all` only.
    LifecycleFailure,
}

/// CCTP chain identifier for specifying source chain.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CctpChain {
    /// Ethereum mainnet (destination: Base)
    Ethereum,
    /// Base mainnet (destination: Ethereum)
    Base,
}

/// Manual position-recovery operations for stuck local CQRS state.
#[derive(Debug, Subcommand)]
pub enum PositionRecoveryCommand {
    /// Release a position's pending offchain order pointer so normal hedging can retry.
    ///
    /// Operates directly on the local CQRS state via aggregate commands; does not
    /// require the running bot.
    ReleaseHedge {
        /// Position symbol (e.g., MSTR)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Pending offchain order ID recorded on the position
        #[arg(short = 'o', long = "order-id")]
        order_id: OffchainOrderId,
        /// Reason to persist on the Position::FailOffChainOrder event (required;
        /// the audit record for this manual intervention)
        #[arg(short = 'r', long = "reason", value_parser = parse_non_empty_reason)]
        reason: String,
    },
    /// Set a position's net exposure after an operator manual correction.
    ///
    /// Operates directly on the local CQRS state via aggregate commands; does not
    /// require the running bot.
    #[command(group(
        ArgGroup::new("target")
            .required(true)
            .multiple(false)
            .args(["zero", "long", "short"])
    ))]
    Set {
        /// Position symbol (e.g., SPYM)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Set the position to exactly zero shares
        #[arg(long = "zero")]
        zero: bool,
        /// Set the position to a positive long net exposure
        #[arg(long = "long", value_parser = parse_positive_shares)]
        long: Option<Positive<FractionalShares>>,
        /// Set the position to a negative short net exposure
        #[arg(long = "short", value_parser = parse_positive_shares)]
        short: Option<Positive<FractionalShares>>,
        /// USDC price per share, required for nonzero targets under a dollar-value threshold
        #[arg(long = "price", value_parser = parse_positive_price)]
        price: Option<Float>,
        /// Operator reason to persist on the Position::ManualPositionAdjusted event
        #[arg(short = 'r', long = "reason", value_parser = parse_non_empty_reason)]
        reason: String,
    },
}

fn parse_float(input: &str) -> Result<Float, String> {
    Float::parse(input.to_string()).map_err(|err| format!("{err}"))
}

/// Parses a strictly-positive price. A zero or negative `--price` would poison
/// `last_price_usdc` and make a dollar-value threshold never trigger a hedge --
/// the exact never-hedges state the price requirement exists to prevent.
fn parse_positive_price(input: &str) -> Result<Float, String> {
    let value = Float::parse(input.to_string()).map_err(|err| format!("{err}"))?;
    let zero = Float::parse("0".to_string()).map_err(|err| format!("{err}"))?;
    if !value.gt(zero).map_err(|err| format!("{err}"))? {
        return Err(format!("--price must be strictly positive, got {value:?}"));
    }
    Ok(value)
}

fn parse_positive_shares(input: &str) -> Result<Positive<FractionalShares>, String> {
    let shares: FractionalShares = input.parse().map_err(|err| format!("{err}"))?;
    Positive::new(shares).map_err(|err| format!("{err}"))
}

/// Rejects a blank `--reason` on event-emitting destructive verbs. clap already
/// requires the flag be present once its default is removed, but a present-but-
/// empty value (`--reason ""` or `--reason "   "`, easy to hit when a shell
/// expands `--reason "$REASON"` to nothing) would persist the same audit-hostile
/// blank reason the requirement exists to prevent.
fn parse_non_empty_reason(input: &str) -> Result<String, String> {
    if input.trim().is_empty() {
        return Err("--reason must not be blank; it is persisted as the audit record".to_string());
    }

    Ok(input.to_string())
}

#[derive(Debug, Parser)]
#[command(name = "st0x-cli")]
#[command(about = "A CLI tool for st0x liquidity operations")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Buy shares of a stock
    Buy {
        /// Stock symbol (e.g., AAPL, TSLA)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Number of shares to buy (must be positive)
        #[arg(short = 'q', long = "quantity", value_parser = parse_positive_shares)]
        quantity: Positive<FractionalShares>,
        /// Time-in-force for the order (day, market-on-close)
        #[arg(long = "time-in-force")]
        time_in_force: Option<TimeInForce>,
        /// Limit price for a manual Alpaca Broker API limit order
        #[arg(long = "limit-price")]
        limit_price: Option<AlpacaLimitPrice>,
        /// Submit the limit order as extended-hours eligible
        #[arg(long = "extended-hours", requires = "limit_price")]
        extended_hours: bool,
    },
    /// Sell shares of a stock
    Sell {
        /// Stock symbol (e.g., AAPL, TSLA)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Number of shares to sell (must be positive)
        #[arg(short = 'q', long = "quantity", value_parser = parse_positive_shares)]
        quantity: Positive<FractionalShares>,
        /// Time-in-force for the order (day, market-on-close)
        #[arg(long = "time-in-force")]
        time_in_force: Option<TimeInForce>,
        /// Limit price for a manual Alpaca Broker API limit order
        #[arg(long = "limit-price")]
        limit_price: Option<AlpacaLimitPrice>,
        /// Submit the limit order as extended-hours eligible
        #[arg(long = "extended-hours", requires = "limit_price")]
        extended_hours: bool,
    },
    /// Account a missed onchain fill by its transaction hash, then place the
    /// opposite-side hedge.
    ///
    /// Recovery tool for fills the bot never recorded: it refuses a fill the bot
    /// has already witnessed in the OnChainTrade log (re-applying would
    /// double-count the position). Run it only when the bot is NOT concurrently
    /// processing the same symbol -- the CLI and the bot run in separate
    /// processes and cannot be serialized by a lock, so a concurrent bot could
    /// still double-account the fill.
    ProcessTx {
        /// Transaction hash (0x prefixed, 64 hex characters)
        #[arg(long = "tx-hash")]
        tx_hash: TxHash,
    },
    /// Transfer tokenized equity between trading venues (Raindex <-> Alpaca)
    ///
    /// Requires Alpaca broker and rebalancing environment variables.
    /// Uses existing MintManager (to-raindex) or RedemptionManager (to-alpaca).
    TransferEquity {
        /// Direction of transfer
        #[arg(short = 'd', long = "direction")]
        direction: TransferDirection,
        /// Stock symbol (e.g., AAPL, TSLA)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Number of shares to transfer (supports fractional shares)
        #[arg(short = 'q', long = "quantity")]
        quantity: FractionalShares,
        /// Issuer request id for mint resume (printed by a fresh to-raindex run)
        #[arg(long = "issuer-request-id")]
        issuer_request_id: Option<Uuid>,
        /// Alpaca redemption wallet (overrides [tokenization] config)
        #[arg(long = "redemption-wallet")]
        redemption_wallet: Option<Address>,
    },

    /// Wrap tokenized equity into wrapped ERC-4626 vault shares
    WrapEquity {
        /// Stock symbol (e.g., AAPL, TSLA)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Number of tokenized shares to wrap (must be positive)
        #[arg(short = 'q', long = "quantity", value_parser = parse_positive_shares)]
        quantity: Positive<FractionalShares>,
    },

    /// Unwrap wrapped ERC-4626 equity shares into the underlying tokenized equity
    UnwrapEquity {
        /// Stock symbol (e.g., AAPL, TSLA)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Number of wrapped shares to unwrap (must be positive)
        #[arg(short = 'q', long = "quantity", value_parser = parse_positive_shares)]
        quantity: Positive<FractionalShares>,
    },

    /// Donate tokenized equity into its ERC-4626 wrapper to bump the wrapper NAV
    ///
    /// A bare transfer of the underlying into the vault raises its share price
    /// (`convertToAssets`) without minting new wrapped shares -- the dividend /
    /// corporate-action NAV bump. Point `--config`/`--secrets` at the dividend
    /// turnkey wallet to fund it from issuance.
    DonateEquity {
        /// Stock symbol (e.g., AAPL, TSLA)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Number of tokenized shares to donate into the wrapper (must be positive)
        #[arg(short = 'q', long = "quantity", value_parser = parse_positive_shares)]
        quantity: Positive<FractionalShares>,
    },

    /// Apply a dividend NAV bump in one step: buy the equity, tokenize it, and
    /// donate it into the wrapper
    ///
    /// Runs buy -> tokenize -> donate in sequence, waiting for each step to
    /// settle (buy fill, tokens onchain, donate receipt). Shares are tokenized
    /// to and donated from the configured `[wallet]`; point `--config` /
    /// `--secrets` at the issuer turnkey wallet.
    DividendBump {
        /// Stock symbol (e.g., AAPL, TSLA)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Number of shares to buy, tokenize, and donate (must be positive)
        #[arg(short = 'q', long = "quantity", value_parser = parse_positive_shares)]
        quantity: Positive<FractionalShares>,
    },

    /// Transfer USDC between trading venues (Raindex <-> Alpaca)
    ///
    /// Requires Alpaca broker and rebalancing environment variables.
    /// Uses Ethereum mainnet and Base mainnet.
    TransferUsdc {
        /// Direction of transfer
        #[arg(short = 'd', long = "direction")]
        direction: TransferDirection,
        /// Amount of USDC to transfer
        #[arg(short = 'a', long = "amount")]
        amount: Usdc,
    },

    /// Mark a pre-burn USDC rebalance as failed, clearing the in-progress guard.
    ///
    /// Valid only from `BridgingSubmitting` or `WithdrawalComplete`. Refused for
    /// any state where a CCTP burn transaction has been submitted. Drives the
    /// aggregate to `BridgingFailed { burn_tx_hash: None }`, which is
    /// non-guard-holding. Guard clears on the next bot restart.
    ///
    /// Safety procedure:
    /// - Stop the bot before running to avoid the concurrent-burn race where the
    ///   bot advances the transfer to `Bridging` between the preflight and the
    ///   send.
    /// - For a `BridgingSubmitting` transfer, verify on-chain that no recent
    ///   CCTP burn left the market-maker wallet before running (a crash at this
    ///   state may have broadcast a burn whose event never persisted).
    /// - Post-burn terminal failures (e.g. `DepositFailed`) use
    ///   `transfer reconcile`. In-flight post-burn states (`Bridging`,
    ///   `AwaitingAttestation`, `Attested`, `Bridged`, `DepositInitiated`)
    ///   should be resumed with `transfer resume --kind usdc`.
    FailUsdcTransfer {
        /// USDC rebalance aggregate ID (UUID)
        #[arg(short = 'i', long = "id")]
        id: Uuid,
        /// Reason for failure (stored in the event for audit purposes)
        #[arg(
            short = 'r',
            long = "reason",
            default_value = "Manually failed via CLI"
        )]
        reason: String,
    },

    /// Clear a recorded pending CCTP burn hash on a transfer stuck at
    /// `BridgingSubmitting` after a `Dropped` burn classification.
    ///
    /// A burn classified `Dropped` latches the aggregate at
    /// `BridgingSubmitting { pending_burn_tx: Some(dropped_tx) }`: it holds the
    /// rebalancing guard and no other recovery command can release it
    /// (`fail-usdc-transfer` rejects it as post-burn, `transfer resume`
    /// re-derives `Dropped`, `transfer reconcile` rejects
    /// `BridgingSubmitting`). This clears the recorded hash, returning the
    /// aggregate to `BridgingSubmitting { pending_burn_tx: None }` so the pre-burn
    /// `fail-usdc-transfer` path can then release the guard.
    ///
    /// ONLY run this AFTER verifying on-chain that the burn never landed (the USDC
    /// never left the market-maker wallet); clearing the hash while a burn is
    /// actually live would let `fail-usdc-transfer` release the guard mid-bridge
    /// and strand the burned funds. Then run `fail-usdc-transfer` to clear the
    /// guard.
    ClearPendingBurn {
        /// USDC rebalance aggregate ID (UUID)
        #[arg(short = 'i', long = "id")]
        id: Uuid,
        /// Why the pending burn is being cleared (shown in CLI output for audit)
        #[arg(
            short = 'r',
            long = "reason",
            default_value = "Burn verified absent on-chain via CLI"
        )]
        reason: String,
    },

    /// Deposit USDC directly to Alpaca from Ethereum (bypasses vault/CCTP)
    ///
    /// This is a simplified command for testing Alpaca integration.
    /// It sends USDC from your Ethereum wallet directly
    /// to Alpaca's deposit address.
    AlpacaDeposit {
        /// Amount of USDC to deposit
        #[arg(short = 'a', long = "amount")]
        amount: Usdc,
    },

    /// Withdraw USDC from Alpaca to a whitelisted address
    ///
    /// Initiates a withdrawal from Alpaca's crypto wallet to a specified address.
    /// The destination address must be whitelisted and approved in Alpaca.
    /// Omit --to to list available whitelisted addresses.
    AlpacaWithdraw {
        /// Amount of USDC to withdraw
        #[arg(short = 'a', long = "amount")]
        amount: Usdc,

        /// Destination address (must be whitelisted; omit to list available)
        #[arg(short = 't', long = "to")]
        to_address: Option<Address>,
    },

    /// Whitelist an address for Alpaca withdrawals
    ///
    /// Addresses must be whitelisted before they can receive withdrawals.
    /// In production, approval typically takes 24 hours.
    /// In sandbox, approval may be instant.
    AlpacaWhitelist {
        /// Address to whitelist (defaults to SENDER_WALLET from env)
        #[arg(short = 'a', long = "address")]
        address: Option<Address>,
    },

    /// List all whitelisted addresses for Alpaca withdrawals
    ///
    /// Shows all whitelist entries with their status, asset, and creation date.
    AlpacaWhitelistList,

    /// Patch travel rule info on all existing whitelisted addresses
    ///
    /// Updates all whitelisted addresses with the beneficiary identity
    /// from [broker.travel_rule] in the config. Required for addresses
    /// whitelisted before the March 27 2026 travel rule deadline.
    AlpacaWhitelistPatchTravelRule,

    /// Remove an address from Alpaca withdrawal whitelist
    ///
    /// Deletes all whitelist entries matching the given address.
    /// Use this to revoke withdrawal access from older wallets.
    AlpacaUnwhitelist {
        /// Address to remove from whitelist
        #[arg(short = 'a', long = "address")]
        address: Address,
    },

    /// List all Alpaca crypto wallet transfers
    ///
    /// Shows all deposits and withdrawals for the Alpaca account.
    /// Useful for debugging transfer status and verifying deposits.
    AlpacaTransfers {
        /// Only show pending transfers
        #[arg(long)]
        pending: bool,
    },

    /// Deposit tokens into a Raindex vault
    ///
    /// This command deposits ERC20 tokens from your wallet into a Raindex OrderBook vault.
    /// It handles ERC20 approval and the vault deposit in sequence, resolving
    /// token decimals from onchain metadata.
    VaultDeposit {
        /// Amount of tokens to deposit (human-readable, e.g., 100 for 100 tokens)
        #[arg(short = 'a', long = "amount", value_parser = parse_float)]
        amount: Float,

        /// Token contract address
        #[arg(short = 't', long = "token")]
        token: Address,

        /// Vault ID
        #[arg(short = 'v', long = "vault-id")]
        vault_id: B256,
    },

    /// Withdraw tokens from a Raindex vault
    ///
    /// This command withdraws ERC20 tokens from a Raindex OrderBook vault to
    /// your wallet, resolving token decimals from onchain metadata.
    VaultWithdraw {
        /// Amount of tokens to withdraw (human-readable, e.g., 100 for 100 tokens)
        #[arg(short = 'a', long = "amount", value_parser = parse_float)]
        amount: Float,

        /// Token contract address
        #[arg(short = 't', long = "token")]
        token: Address,

        /// Vault ID
        #[arg(short = 'v', long = "vault-id")]
        vault_id: B256,
    },

    /// Withdraw USDC from the configured Raindex cash vault
    ///
    /// This preserves the existing USDC-specific operator flow by resolving
    /// `assets.cash.vault_ids` from config and forwarding into the generic
    /// vault withdrawal implementation.
    VaultWithdrawUsdc {
        /// Amount of USDC to withdraw
        #[arg(short = 'a', long = "amount")]
        amount: Usdc,
    },

    /// Bridge USDC via CCTP (full flow: burn -> attestation -> mint)
    ///
    /// Bridges USDC between Ethereum mainnet and Base using Circle's CCTP V2.
    CctpBridge {
        /// Amount of USDC to bridge (omit to use --all)
        #[arg(short = 'a', long = "amount", conflicts_with = "all")]
        amount: Option<Usdc>,
        /// Bridge entire USDC balance
        #[arg(long = "all", conflicts_with = "amount")]
        all: bool,
        /// Source chain to burn from
        #[arg(long = "from")]
        from: CctpChain,
    },

    /// Reset USDC allowance for the orderbook to zero.
    ///
    /// Use this to investigate approval behavior or when switching orderbook addresses.
    ResetAllowance {
        /// Chain where to reset allowance
        #[arg(long = "chain")]
        chain: CctpChain,
    },

    /// Request tokenization of shares via Alpaca (isolated test command)
    ///
    /// Calls the Alpaca tokenization API to convert offchain shares to onchain tokens.
    /// This is an isolated test command that only interacts with Alpaca's API,
    /// without any Raindex/vault operations.
    AlpacaTokenize {
        /// Stock symbol (e.g., AAPL, TSLA) -- resolves the tokenized-equity
        /// address from `[assets.equities]`
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Number of shares to tokenize (supports fractional shares)
        #[arg(short = 'q', long = "quantity")]
        quantity: FractionalShares,
        /// Recipient wallet address (defaults to configured wallet address)
        #[arg(short = 'r', long = "recipient")]
        recipient: Option<Address>,
    },

    /// Request redemption of tokenized shares via Alpaca (isolated test command)
    ///
    /// Calls the Alpaca tokenization API to convert onchain tokens back to offchain shares.
    /// This is an isolated test command that only interacts with Alpaca's API,
    /// without any Raindex/vault operations.
    AlpacaRedeem {
        /// Stock symbol (e.g., AAPL, TSLA) -- resolves the tokenized-equity
        /// address from `[assets.equities]`
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Number of shares to redeem (supports fractional shares)
        #[arg(short = 'q', long = "quantity")]
        quantity: FractionalShares,
        /// Alpaca redemption wallet (overrides [tokenization] config)
        #[arg(long = "redemption-wallet")]
        redemption_wallet: Option<Address>,
    },

    /// Convert USDC to/from USD on Alpaca
    ///
    /// Uses the USDC/USD trading pair to convert between USDC (crypto) and USD (buying power).
    /// - `to-usd`: Sell USDC for USD buying power
    /// - `to-usdc`: Buy USDC with USD buying power
    AlpacaConvert {
        /// Conversion direction
        #[arg(short = 'd', long = "direction")]
        direction: ConvertDirection,
        /// Amount of USDC to convert
        #[arg(short = 'a', long = "amount")]
        amount: Usdc,
    },

    /// Journal (transfer) equities between Alpaca accounts
    ///
    /// Creates a security journal (JNLS) to move shares from the configured
    /// account to a destination account. Both accounts must be under the
    /// same Alpaca broker firm.
    AlpacaJournal {
        /// Destination account ID (UUID)
        #[arg(long = "to")]
        destination: AlpacaAccountId,
        /// Stock symbol (e.g., AAPL, TSLA)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Number of shares to transfer (must be positive, supports fractional)
        #[arg(short = 'q', long = "quantity", value_parser = parse_positive_shares)]
        quantity: Positive<FractionalShares>,
    },

    /// List all Alpaca tokenization requests
    ///
    /// Shows mint and redemption requests for the Alpaca account.
    /// Useful for debugging tokenization status without creating new requests.
    AlpacaTokenizationRequests,

    /// Check the status of a broker order by order ID
    OrderStatus {
        /// The broker order ID to check
        #[arg(long = "order-id")]
        order_id: String,
    },

    /// Submit raw calldata transactions to the blockchain
    ///
    /// Reads transactions from stdin (pipe mode) or from --to/--data flags.
    /// Stdin format: one `<address>:<hex_calldata>` per line.
    /// Signs via the configured Turnkey wallet and prompts before submitting.
    Submit {
        /// Target contract address (flag mode, mutually exclusive with stdin)
        #[arg(long = "to", requires = "data")]
        to: Option<Address>,

        /// Hex-encoded calldata (flag mode, mutually exclusive with stdin)
        #[arg(long = "data", requires = "to")]
        data: Option<String>,

        /// Skip confirmation prompt
        #[arg(long = "yes")]
        yes: bool,
    },

    /// Recover stuck positions through aggregate commands.
    Position {
        #[command(subcommand)]
        command: PositionRecoveryCommand,
    },

    /// Recover stuck asset transfers (USDC rebalances, mints, redemptions).
    Transfer {
        #[command(subcommand)]
        command: TransferCommand,
    },

    /// Maintain materialized views.
    View {
        #[command(subcommand)]
        command: ViewCommand,
    },

    /// Recover stuck CCTP (cross-chain USDC) transfers.
    Cctp {
        #[command(subcommand)]
        command: CctpCommand,
    },
}

/// Recover stuck asset transfers between trading venues.
#[derive(Debug, Subcommand)]
pub enum TransferCommand {
    /// Resume interrupted transfers.
    ///
    /// `--kind usdc` re-drives a single USDC transfer whose CLI invocation was
    /// interrupted after the burn: pass `--id` (printed by `transfer-usdc`) and
    /// the original `--direction`. An unknown id is rejected (never starts a
    /// fresh burn); the resume uses the persisted amount. Operates on the local
    /// CQRS state plus a live RPC provider
    /// (re-drives the on-chain burn/mint/deposit flow itself); the bot must not
    /// be concurrently driving the same id.
    ///
    /// `--kind equity` resumes ALL interrupted mints and redemptions via the
    /// running bot's REST API: there is no per-id filter (`--id`/`--direction`
    /// are rejected), each transfer succeeds or fails independently, and
    /// failures are reported as counts (non-zero exit). Requires the bot to be
    /// running and serving its API on the configured `server_port`.
    Resume {
        /// What to resume: a single USDC transfer (`usdc`) or all equity transfers
        /// (`equity`). Required -- clap `required_if_eq` only enforces the
        /// `--id`/`--direction` dependency against an explicit value, not a
        /// default, so `--kind` is not defaulted.
        #[arg(short = 'k', long = "kind", value_enum)]
        kind: TransferResumeKind,
        /// Id of the USDC transfer to resume (required for `--kind usdc`)
        #[arg(long = "id", required_if_eq("kind", "usdc"))]
        id: Option<Uuid>,
        /// Direction of the original USDC transfer (required for `--kind usdc`)
        #[arg(short = 'd', long = "direction", required_if_eq("kind", "usdc"))]
        direction: Option<TransferDirection>,
    },

    /// Reconcile a transfer stuck in a terminal failure to a resolved terminal.
    ///
    /// `--kind usdc` drives a USDC rebalance stranded in a post-burn terminal
    /// failure that strands the in-progress guard (`DepositFailed`, a post-burn
    /// `BridgingFailed`, or a `BaseToAlpaca` `ConversionFailed`) to the clearing
    /// terminal `Reconciled` state (the funds were handled out-of-band).
    /// `--kind mint` / `--kind redemption` mark an equity transfer stuck in
    /// `Failed` as `Reconciled` once its residue was handled out-of-band (e.g.
    /// via wrap-equity/vault-deposit) -- a pure bookkeeping resolution. Rejects
    /// an unknown id or an aggregate not in the expected failure state. Operates
    /// directly on the local CQRS state; does not require the running bot, but
    /// the bot must not be concurrently driving the same id.
    Reconcile {
        /// What to reconcile: `usdc`, `mint`, or `redemption`
        #[arg(short = 'k', long = "kind")]
        kind: ReconcileKind,
        /// Aggregate id (USDC rebalance id for `usdc`, issuer_request_id for
        /// `mint`, redemption id for `redemption`)
        #[arg(long = "id")]
        id: String,
        /// Why the transfer is being reconciled (required; persisted as the audit
        /// record). For `usdc` this must be one of `funds-moved-manually` or
        /// `deposit-credited-offline`; for `mint`/`redemption` it is free text.
        #[arg(short = 'r', long = "reason", value_parser = parse_non_empty_reason)]
        reason: String,
    },

    /// Manually fail a stuck mint or redemption transfer.
    ///
    /// Marks a transfer aggregate as failed, transitioning it to a terminal state.
    /// Use when a transfer is permanently stuck and needs operator intervention.
    /// Operates directly on the local CQRS state; does not require the running
    /// bot, but the bot must not be concurrently driving the same id.
    Fail {
        /// Transfer type: "mint" or "redemption"
        #[arg(short = 'k', long = "kind", alias = "type", short_alias = 't')]
        kind: TransferType,
        /// Aggregate ID (issuer_request_id for mint, redemption ID for redemption)
        #[arg(short = 'i', long = "id")]
        id: String,
        /// Reason for failure (required; persisted as the audit record)
        #[arg(short = 'r', long = "reason", value_parser = parse_non_empty_reason)]
        reason: String,
    },

    /// Re-check a failed mint or redemption and complete it if the provider settled it.
    ///
    /// Delegates to the running bot's REST API so recovery dispatches through the
    /// in-process reactor (correcting live inventory). Requires the bot to be
    /// running and serving its API on the configured `server_port`.
    Recheck {
        /// Transfer type: "mint" or "redemption"
        #[arg(short = 'k', long = "kind", alias = "type", short_alias = 't')]
        kind: TransferType,
        /// Aggregate ID (issuer_request_id for mint, redemption ID for redemption)
        #[arg(short = 'i', long = "id")]
        id: String,
    },
}

/// Maintain materialized views derived from the event log.
#[derive(Debug, Subcommand)]
pub enum ViewCommand {
    /// Rebuild a materialized view by replaying all events from scratch.
    ///
    /// Use as an escape hatch when a view becomes corrupted (e.g., due to lost
    /// updates from optimistic lock conflicts). Deletes the view row(s) and
    /// replays all events to reconstruct correct state. Operates directly on the
    /// local CQRS state; does not require the running bot.
    Rebuild {
        /// Aggregate type to rebuild (position, offchain-order, vault-registry)
        #[arg(short = 'a', long = "aggregate")]
        aggregate: AggregateView,
        /// Specific aggregate ID to rebuild (e.g., AAPL for position).
        /// Mutually exclusive with --all.
        #[arg(long = "id", conflicts_with = "all", required_unless_present = "all")]
        id: Option<String>,
        /// Rebuild all views for the aggregate type.
        /// Mutually exclusive with --id.
        #[arg(long = "all", conflicts_with = "id", required_unless_present = "id")]
        all: bool,
    },
}

/// Recover stuck CCTP (cross-chain USDC) transfers.
#[derive(Debug, Subcommand)]
pub enum CctpCommand {
    /// Complete the mint of a stuck CCTP transfer on the destination chain.
    ///
    /// Use this when a CCTP burn succeeded but the mint wasn't completed (e.g.,
    /// due to attestation polling being interrupted). Provide the burn
    /// transaction hash and specify the source chain to recover the transfer.
    ///
    /// Live RPC only: touches no database state or aggregate. Ensure the bot is
    /// not concurrently driving the same on-chain mint. After completing the
    /// mint, bring a stuck `UsdcRebalance` aggregate back in sync: `transfer
    /// resume` while it is still non-terminal (it adopts the existing mint), or
    /// `transfer reconcile` if it already reached a post-burn terminal failure.
    CompleteMint {
        /// Transaction hash of the burn transaction on the source chain
        #[arg(long = "burn-tx")]
        burn_tx: TxHash,
        /// Source chain where the burn occurred
        #[arg(long = "source-chain")]
        source_chain: CctpChain,
    },
}

#[derive(Debug, Parser)]
#[command(name = "st0x-cli")]
#[command(about = "A CLI tool for st0x liquidity operations")]
#[command(version)]
pub struct CliEnv {
    #[clap(flatten)]
    env: Env,
    #[command(subcommand)]
    pub command: Commands,
}

impl CliEnv {
    /// Parse CLI arguments, load config from file, and return with subcommand.
    pub async fn parse_and_convert() -> anyhow::Result<(Ctx, Commands)> {
        Self::parse().load().await
    }

    /// Load config and secrets from the file paths parsed from CLI arguments.
    pub(crate) async fn load(self) -> anyhow::Result<(Ctx, Commands)> {
        let ctx = Ctx::load_files(&self.env.config, &self.env.secrets).await?;
        Ok((ctx, self.command))
    }
}

pub async fn run(ctx: Ctx) -> anyhow::Result<()> {
    let cli = Cli::parse();
    let pool = ctx.get_sqlite_pool().await?;
    run_command_with_writers(ctx, cli.command, &pool, &mut std::io::stdout()).await
}

pub async fn run_command(ctx: Ctx, command: Commands) -> anyhow::Result<()> {
    let pool = ctx.get_sqlite_pool().await?;
    run_command_with_writers(ctx, command, &pool, &mut std::io::stdout()).await
}

async fn execute_order<W: Write>(
    request: trading::CliOrderRequest,
    ctx: &Ctx,
    pool: &SqlitePool,
    stdout: &mut W,
) -> anyhow::Result<()> {
    info!(
        "Processing {:?} order: symbol={}, quantity={}",
        request.direction, request.symbol, request.shares
    );
    trading::execute_order_with_writers(request, ctx, pool, stdout).await
}

/// Commands that don't require an RPC provider.
enum SimpleCommand {
    Buy {
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
        time_in_force: Option<TimeInForce>,
        limit_price: Option<AlpacaLimitPrice>,
        extended_hours: bool,
    },
    Sell {
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
        time_in_force: Option<TimeInForce>,
        limit_price: Option<AlpacaLimitPrice>,
        extended_hours: bool,
    },
    TransferEquity {
        direction: TransferDirection,
        symbol: Symbol,
        quantity: FractionalShares,
        issuer_request_id: Option<Uuid>,
        redemption_wallet: Option<Address>,
    },
    WrapEquity {
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
    },
    UnwrapEquity {
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
    },
    DonateEquity {
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
    },
    AlpacaDeposit {
        amount: Usdc,
    },
    AlpacaWithdraw {
        amount: Usdc,
        to_address: Option<Address>,
    },
    AlpacaWhitelist {
        address: Option<Address>,
    },
    AlpacaWhitelistList,
    AlpacaWhitelistPatchTravelRule,
    AlpacaUnwhitelist {
        address: Address,
    },
    AlpacaTransfers {
        pending: bool,
    },
    AlpacaConvert {
        direction: ConvertDirection,
        amount: Usdc,
    },
    AlpacaJournal {
        destination: AlpacaAccountId,
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
    },
    VaultDeposit {
        amount: Float,
        token: Address,
        vault_id: B256,
    },
    VaultWithdraw {
        amount: Float,
        token: Address,
        vault_id: B256,
    },
    VaultWithdrawUsdc {
        amount: Usdc,
    },
    OrderStatus {
        order_id: String,
    },
    Submit {
        to: Option<Address>,
        data: Option<String>,
        yes: bool,
    },
    RebuildView {
        aggregate: AggregateView,
        id: Option<String>,
        all: bool,
    },
    Position {
        command: PositionRecoveryCommand,
    },
    FailTransfer {
        transfer_type: TransferType,
        id: String,
        reason: String,
    },
    RecheckTransfer {
        transfer_type: TransferType,
        id: String,
    },
    ResumeInterruptedTransfers,
    ReconcileUsdcTransfer {
        /// Raw operator-supplied `--id` from `transfer reconcile --kind usdc`,
        /// parsed into a `Uuid` in the handler so a malformed id surfaces a
        /// clear operator error.
        id: String,
        /// Raw operator-supplied `--reason` from `transfer reconcile --kind
        /// usdc`, parsed into [`ReconcileReasonArg`] in the handler so an
        /// unknown value surfaces a clear error.
        reason: String,
    },
    FailUsdcTransfer {
        id: Uuid,
        reason: String,
    },
    ReconcileEquityTransfer {
        transfer_type: TransferType,
        id: String,
        reason: String,
    },
    ClearPendingBurn {
        id: Uuid,
        reason: String,
    },
}

/// Parses the operator `--reason` string for a USDC reconcile into the typed
/// [`ReconcileReasonArg`]. Returns an error for an unknown reason so the
/// operator gets a clear message instead of a silent fallback.
fn parse_usdc_reconcile_reason(reason: &str) -> anyhow::Result<ReconcileReasonArg> {
    ReconcileReasonArg::from_str(reason, false).map_err(|_| {
        anyhow::anyhow!(
            "transfer reconcile --kind usdc: unknown --reason {reason:?} \
             (expected `funds-moved-manually` or `deposit-credited-offline`)"
        )
    })
}

#[cfg(feature = "test-support")]
pub async fn fail_transfer_for_test(
    pool: &SqlitePool,
    transfer_type: TransferType,
    id: &str,
    reason: &str,
) -> anyhow::Result<()> {
    let mut stdout = Vec::new();
    rebalancing::fail_transfer_command(&mut stdout, pool, transfer_type, id, reason).await
}

#[cfg(feature = "test-support")]
pub async fn recheck_transfer_for_test(
    ctx: &Ctx,
    transfer_type: TransferType,
    id: &str,
) -> anyhow::Result<()> {
    let mut stdout = Vec::new();
    rebalancing::recheck_transfer_command(&mut stdout, transfer_type, id, ctx).await
}

/// Seeds a `TokenizedEquityMint` aggregate at `TokensWrapped`.
///
/// Inserts the canonical `MintRequested -> MintAccepted -> TokensReceived ->
/// WrapSubmitted -> TokensWrapped` event sequence directly, bypassing the
/// command handlers so no broker/tokenization services are invoked.
///
/// Used by the active-mint wrapped-equity recovery e2e to set up an active
/// mint stuck after wrapping but before the Raindex deposit. Once the bot
/// starts, its startup recovery detects the interrupted mint and drives it
/// through `resume_mint`.
#[cfg(feature = "test-support")]
pub async fn seed_mint_at_tokens_wrapped_for_test(
    pool: &SqlitePool,
    mint_id_str: &str,
    symbol_str: &str,
    wallet: Address,
    wrap_tx_hash: TxHash,
    wrapped_shares: alloy::primitives::U256,
    quantity: Float,
) -> anyhow::Result<()> {
    use chrono::Utc;
    use st0x_event_sorcery::DomainEvent;

    use crate::tokenized_equity_mint::{
        IssuerRequestId, TokenizationRequestId, TokenizedEquityMintEvent,
    };

    let symbol = Symbol::new(symbol_str.to_string())?;
    let mint_id: IssuerRequestId = mint_id_str.parse()?;
    let now = Utc::now();

    let events = [
        TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity,
            wallet,
            requested_at: now,
        },
        TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: mint_id.clone(),
            tokenization_request_id: TokenizationRequestId(
                "seeded-tokenization-request-id".to_string(),
            ),
            accepted_at: now,
        },
        TokenizedEquityMintEvent::TokensReceived {
            tx_hash: TxHash::random(),
            shares_minted: wrapped_shares,
            fees: None,
            received_at: now,
        },
        TokenizedEquityMintEvent::WrapSubmitted {
            wrap_tx_hash,
            submitted_at: now,
        },
        TokenizedEquityMintEvent::TokensWrapped {
            wrap_tx_hash,
            wrapped_shares,
            wrapped_at: now,
            wrap_block: None,
        },
    ];

    let IssuerRequestId(raw_id) = &mint_id;
    for (index, event) in events.iter().enumerate() {
        let payload = serde_json::to_string(event)?;
        let sequence = i64::try_from(index + 1)?;
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES ('TokenizedEquityMint', ?, ?, ?, ?, ?, '{}')",
        )
        .bind(raw_id.to_string())
        .bind(sequence)
        .bind(event.event_type())
        .bind(event.event_version())
        .bind(payload)
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Commands that require an RPC provider.
enum ProviderCommand {
    ProcessTx {
        tx_hash: TxHash,
    },
    TransferUsdc {
        direction: TransferDirection,
        amount: Usdc,
    },
    ResumeUsdcTransfer {
        id: Uuid,
        direction: TransferDirection,
    },
    CctpBridge {
        amount: Option<Usdc>,
        all: bool,
        from: CctpChain,
    },
    CctpRecover {
        burn_tx: TxHash,
        source_chain: CctpChain,
    },
    ResetAllowance {
        chain: CctpChain,
    },
    AlpacaTokenize {
        symbol: Symbol,
        quantity: FractionalShares,
        recipient: Option<Address>,
    },
    AlpacaRedeem {
        symbol: Symbol,
        quantity: FractionalShares,
        redemption_wallet: Option<Address>,
    },
    DividendBump {
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
    },
    AlpacaTokenizationRequests,
}

/// Rejects argument combinations clap cannot express conditionally: the
/// equity bulk-resume takes no per-id filter, so a supplied `--id` or
/// `--direction` signals the operator probably meant `--kind usdc` and must
/// not be silently discarded.
fn validate_command(command: &Commands) -> anyhow::Result<()> {
    let Commands::Transfer {
        command:
            TransferCommand::Resume {
                kind,
                id,
                direction,
                ..
            },
    } = command
    else {
        return Ok(());
    };

    match kind {
        TransferResumeKind::Equity if id.is_some() || direction.is_some() => {
            anyhow::bail!(
                "transfer resume --kind equity resumes ALL interrupted equity transfers and \
                 takes no --id/--direction. To resume a single USDC transfer, re-run with \
                 --kind usdc."
            );
        }
        TransferResumeKind::Usdc if id.is_none() || direction.is_none() => {
            anyhow::bail!(
                "transfer resume --kind usdc requires --id and --direction (clap should \
                 have rejected this invocation; please report it)"
            );
        }
        TransferResumeKind::Equity | TransferResumeKind::Usdc => Ok(()),
    }
}

async fn run_command_with_writers<W: Write>(
    ctx: Ctx,
    command: Commands,
    pool: &SqlitePool,
    stdout: &mut W,
) -> anyhow::Result<()> {
    validate_command(&command)?;
    match classify_command(command) {
        Ok(simple) => run_simple_command(simple, &ctx, pool, stdout).await?,
        Err(provider_cmd) => {
            let order_placer = trading::create_order_placer(&ctx, pool);
            run_provider_command(provider_cmd, &ctx, pool, stdout, order_placer).await?;
        }
    }

    info!("CLI operation completed successfully");
    Ok(())
}

// One flat match mapping every CLI command to its internal Simple/Provider
// dispatch. Kept as a single match (per the repo's "don't split simple-but-long
// matches" rule) rather than fragmented into per-group helpers.
fn classify_command(command: Commands) -> Result<SimpleCommand, ProviderCommand> {
    match command {
        Commands::Buy {
            symbol,
            quantity,
            time_in_force,
            limit_price,
            extended_hours,
        } => Ok(SimpleCommand::Buy {
            symbol,
            quantity,
            time_in_force,
            limit_price,
            extended_hours,
        }),
        Commands::Sell {
            symbol,
            quantity,
            time_in_force,
            limit_price,
            extended_hours,
        } => Ok(SimpleCommand::Sell {
            symbol,
            quantity,
            time_in_force,
            limit_price,
            extended_hours,
        }),
        Commands::TransferEquity {
            direction,
            symbol,
            quantity,
            issuer_request_id,
            redemption_wallet,
        } => Ok(SimpleCommand::TransferEquity {
            direction,
            symbol,
            quantity,
            issuer_request_id,
            redemption_wallet,
        }),
        Commands::WrapEquity { symbol, quantity } => {
            Ok(SimpleCommand::WrapEquity { symbol, quantity })
        }
        Commands::UnwrapEquity { symbol, quantity } => {
            Ok(SimpleCommand::UnwrapEquity { symbol, quantity })
        }
        Commands::DonateEquity { symbol, quantity } => {
            Ok(SimpleCommand::DonateEquity { symbol, quantity })
        }
        Commands::AlpacaDeposit { amount } => Ok(SimpleCommand::AlpacaDeposit { amount }),
        Commands::AlpacaWithdraw { amount, to_address } => {
            Ok(SimpleCommand::AlpacaWithdraw { amount, to_address })
        }
        Commands::AlpacaWhitelist { address } => Ok(SimpleCommand::AlpacaWhitelist { address }),
        Commands::AlpacaWhitelistList => Ok(SimpleCommand::AlpacaWhitelistList),
        Commands::AlpacaWhitelistPatchTravelRule => {
            Ok(SimpleCommand::AlpacaWhitelistPatchTravelRule)
        }
        Commands::AlpacaUnwhitelist { address } => Ok(SimpleCommand::AlpacaUnwhitelist { address }),
        Commands::AlpacaTransfers { pending } => Ok(SimpleCommand::AlpacaTransfers { pending }),
        Commands::AlpacaConvert { direction, amount } => {
            Ok(SimpleCommand::AlpacaConvert { direction, amount })
        }
        Commands::AlpacaJournal {
            destination,
            symbol,
            quantity,
        } => Ok(SimpleCommand::AlpacaJournal {
            destination,
            symbol,
            quantity,
        }),
        Commands::AlpacaTokenizationRequests => Err(ProviderCommand::AlpacaTokenizationRequests),
        Commands::ProcessTx { tx_hash } => Err(ProviderCommand::ProcessTx { tx_hash }),
        Commands::TransferUsdc { direction, amount } => {
            Err(ProviderCommand::TransferUsdc { direction, amount })
        }
        Commands::VaultDeposit {
            amount,
            token,
            vault_id,
        } => Ok(SimpleCommand::VaultDeposit {
            amount,
            token,
            vault_id,
        }),
        Commands::VaultWithdraw {
            amount,
            token,
            vault_id,
        } => Ok(SimpleCommand::VaultWithdraw {
            amount,
            token,
            vault_id,
        }),
        Commands::VaultWithdrawUsdc { amount } => Ok(SimpleCommand::VaultWithdrawUsdc { amount }),
        Commands::CctpBridge { amount, all, from } => {
            Err(ProviderCommand::CctpBridge { amount, all, from })
        }
        Commands::ResetAllowance { chain } => Err(ProviderCommand::ResetAllowance { chain }),
        Commands::AlpacaTokenize {
            symbol,
            quantity,
            recipient,
        } => Err(ProviderCommand::AlpacaTokenize {
            symbol,
            quantity,
            recipient,
        }),
        Commands::AlpacaRedeem {
            symbol,
            quantity,
            redemption_wallet,
        } => Err(ProviderCommand::AlpacaRedeem {
            symbol,
            quantity,
            redemption_wallet,
        }),
        Commands::DividendBump { symbol, quantity } => {
            Err(ProviderCommand::DividendBump { symbol, quantity })
        }
        Commands::OrderStatus { order_id } => Ok(SimpleCommand::OrderStatus { order_id }),
        Commands::Submit { to, data, yes } => Ok(SimpleCommand::Submit { to, data, yes }),
        Commands::Position { command } => Ok(SimpleCommand::Position { command }),
        Commands::FailUsdcTransfer { id, reason } => {
            Ok(SimpleCommand::FailUsdcTransfer { id, reason })
        }
        Commands::Transfer { command } => match command {
            // `--kind equity` resumes all interrupted equity transfers via the bot
            // REST API. `--kind usdc` re-drives a single USDC transfer; `id` and
            // `direction` are guaranteed present by `required_if_eq("kind","usdc")`.
            TransferCommand::Resume {
                kind: TransferResumeKind::Equity,
                ..
            } => Ok(SimpleCommand::ResumeInterruptedTransfers),
            TransferCommand::Resume {
                kind: TransferResumeKind::Usdc,
                id: Some(id),
                direction: Some(direction),
            } => Err(ProviderCommand::ResumeUsdcTransfer { id, direction }),
            TransferCommand::Resume {
                kind: TransferResumeKind::Usdc,
                ..
            } => {
                // Doubly guarded: clap's `required_if_eq("kind","usdc")`
                // rejects the missing args at parse time, and
                // `validate_command` (which also checks this combination)
                // runs before classify in the production dispatch. Callers
                // invoking classify directly must pass clap-parsed input.
                unreachable!("clap `required_if_eq(\"kind\",\"usdc\")` guarantees id and direction")
            }
            // Classification stays pure (routing only): the usdc id/reason are
            // parsed and validated in the handler so a bad value surfaces a clear
            // operator error rather than a panic here.
            TransferCommand::Reconcile { kind, id, reason } => match kind {
                ReconcileKind::Usdc => Ok(SimpleCommand::ReconcileUsdcTransfer { id, reason }),
                ReconcileKind::Mint => Ok(SimpleCommand::ReconcileEquityTransfer {
                    transfer_type: TransferType::Mint,
                    id,
                    reason,
                }),
                ReconcileKind::Redemption => Ok(SimpleCommand::ReconcileEquityTransfer {
                    transfer_type: TransferType::Redemption,
                    id,
                    reason,
                }),
            },
            TransferCommand::Fail { kind, id, reason } => Ok(SimpleCommand::FailTransfer {
                transfer_type: kind,
                id,
                reason,
            }),
            TransferCommand::Recheck { kind, id } => Ok(SimpleCommand::RecheckTransfer {
                transfer_type: kind,
                id,
            }),
        },
        Commands::View { command } => match command {
            ViewCommand::Rebuild { aggregate, id, all } => {
                Ok(SimpleCommand::RebuildView { aggregate, id, all })
            }
        },
        Commands::Cctp { command } => match command {
            CctpCommand::CompleteMint {
                burn_tx,
                source_chain,
            } => Err(ProviderCommand::CctpRecover {
                burn_tx,
                source_chain,
            }),
        },
        Commands::ClearPendingBurn { id, reason } => {
            Ok(SimpleCommand::ClearPendingBurn { id, reason })
        }
    }
}

async fn run_simple_command<W: Write>(
    command: SimpleCommand,
    ctx: &Ctx,
    pool: &SqlitePool,
    stdout: &mut W,
) -> anyhow::Result<()> {
    match command {
        SimpleCommand::Buy {
            symbol,
            quantity,
            time_in_force,
            limit_price,
            extended_hours,
        } => {
            let request = trading::CliOrderRequest::from_cli_args(
                symbol,
                quantity,
                Direction::Buy,
                time_in_force,
                limit_price,
                extended_hours,
            )
            .map_err(|error| {
                let _ = writeln!(stdout, "❌ Failed to place order: {error}");
                error
            })?;
            execute_order(request, ctx, pool, stdout).await
        }
        SimpleCommand::Sell {
            symbol,
            quantity,
            time_in_force,
            limit_price,
            extended_hours,
        } => {
            let request = trading::CliOrderRequest::from_cli_args(
                symbol,
                quantity,
                Direction::Sell,
                time_in_force,
                limit_price,
                extended_hours,
            )
            .map_err(|error| {
                let _ = writeln!(stdout, "❌ Failed to place order: {error}");
                error
            })?;
            execute_order(request, ctx, pool, stdout).await
        }
        SimpleCommand::TransferEquity {
            direction,
            symbol,
            quantity,
            issuer_request_id,
            redemption_wallet,
        } => {
            rebalancing::transfer_equity_command(
                stdout,
                direction,
                &symbol,
                quantity,
                issuer_request_id,
                redemption_wallet,
                ctx,
                pool,
            )
            .await
        }
        SimpleCommand::WrapEquity { symbol, quantity } => {
            wrapper::wrap_equity_command(stdout, symbol, quantity, ctx).await
        }
        SimpleCommand::UnwrapEquity { symbol, quantity } => {
            wrapper::unwrap_equity_command(stdout, symbol, quantity, ctx).await
        }
        SimpleCommand::DonateEquity { symbol, quantity } => {
            wrapper::donate_equity_command(stdout, symbol, quantity, ctx).await
        }
        SimpleCommand::AlpacaDeposit { amount } => {
            alpaca_wallet::alpaca_deposit_command::<OpenChainErrorRegistry, _>(stdout, amount, ctx)
                .await
        }
        SimpleCommand::AlpacaWhitelist { address } => {
            alpaca_wallet::alpaca_whitelist_command(stdout, address, ctx).await
        }
        SimpleCommand::AlpacaWhitelistList => {
            alpaca_wallet::alpaca_whitelist_list_command(stdout, ctx).await
        }
        SimpleCommand::AlpacaWhitelistPatchTravelRule => {
            alpaca_wallet::alpaca_whitelist_patch_travel_rule_command(stdout, ctx).await
        }
        SimpleCommand::AlpacaUnwhitelist { address } => {
            alpaca_wallet::alpaca_unwhitelist_command(stdout, address, ctx).await
        }
        SimpleCommand::AlpacaWithdraw { amount, to_address } => {
            alpaca_wallet::alpaca_withdraw_command::<OpenChainErrorRegistry, _>(
                stdout, amount, to_address, ctx,
            )
            .await
        }
        SimpleCommand::AlpacaTransfers { pending } => {
            alpaca_wallet::alpaca_transfers_command(stdout, pending, ctx).await
        }
        SimpleCommand::AlpacaConvert { direction, amount } => {
            alpaca_wallet::alpaca_convert_command(stdout, direction, amount, ctx).await
        }
        SimpleCommand::AlpacaJournal {
            destination,
            symbol,
            quantity,
        } => {
            alpaca_wallet::alpaca_journal_command(stdout, destination, symbol, quantity, ctx).await
        }
        SimpleCommand::VaultDeposit {
            amount,
            token,
            vault_id,
        } => {
            let deposit = vault::Deposit {
                amount,
                token,
                vault_id,
            };
            vault::vault_deposit_command(stdout, deposit, ctx).await
        }
        SimpleCommand::VaultWithdraw {
            amount,
            token,
            vault_id,
        } => {
            let withdraw = vault::Withdraw {
                amount,
                token,
                vault_id,
            };
            vault::vault_withdraw_command(stdout, withdraw, ctx).await
        }
        SimpleCommand::VaultWithdrawUsdc { amount } => {
            vault::vault_withdraw_usdc_command(stdout, amount, ctx).await
        }
        SimpleCommand::OrderStatus { order_id } => {
            trading::order_status_command(stdout, &order_id, ctx, pool).await
        }
        SimpleCommand::Submit { to, data, yes } => {
            let transactions = if let (Some(to), Some(data)) = (to, data) {
                vec![submit::parse_flag_transaction(to, &data)?]
            } else {
                let stdin = std::io::stdin().lock();
                submit::parse_stdin_lines(stdin)?
            };
            submit::submit_command(stdout, transactions, yes, ctx).await
        }
        SimpleCommand::RebuildView { aggregate, id, all } => {
            rebuild_view(stdout, pool, aggregate, id, all).await
        }
        SimpleCommand::Position { command } => {
            run_position_command(stdout, pool, command, ctx.execution_threshold).await
        }
        SimpleCommand::FailTransfer {
            transfer_type,
            id,
            reason,
        } => rebalancing::fail_transfer_command(stdout, pool, transfer_type, &id, &reason).await,
        SimpleCommand::RecheckTransfer { transfer_type, id } => {
            rebalancing::recheck_transfer_command(stdout, transfer_type, &id, ctx).await
        }
        SimpleCommand::ResumeInterruptedTransfers => {
            rebalancing::resume_interrupted_transfers_command(stdout, ctx).await
        }
        SimpleCommand::ReconcileUsdcTransfer { id, reason } => {
            let id = id.parse::<Uuid>().map_err(|error| {
                anyhow::anyhow!("transfer reconcile --kind usdc: invalid id {id:?}: {error}")
            })?;
            // A blank reason was already rejected by `parse_non_empty_reason` at
            // clap parse time; this enum-domain parse is the secondary check that
            // also constrains the value to the usdc vocabulary.
            let reason = parse_usdc_reconcile_reason(&reason)?;
            rebalancing::reconcile_usdc_transfer_command(stdout, id, reason.into(), pool).await
        }
        SimpleCommand::FailUsdcTransfer { id, reason } => {
            rebalancing::fail_usdc_transfer_command(stdout, id, &reason, pool).await
        }
        SimpleCommand::ReconcileEquityTransfer {
            transfer_type,
            id,
            reason,
        } => {
            rebalancing::reconcile_equity_transfer_command(stdout, transfer_type, &id, reason, pool)
                .await
        }
        SimpleCommand::ClearPendingBurn { id, reason } => {
            rebalancing::clear_pending_burn_command(stdout, id, &reason, pool).await
        }
    }
}

async fn rebuild_view<W: Write>(
    stdout: &mut W,
    pool: &SqlitePool,
    aggregate: AggregateView,
    id: Option<String>,
    all: bool,
) -> anyhow::Result<()> {
    match aggregate {
        AggregateView::Position => {
            let projection = Projection::<Position>::sqlite(pool.clone());

            if let Some(raw_id) = id {
                let symbol: Symbol = raw_id.parse()?;
                projection.rebuild(&symbol).await?;
                writeln!(stdout, "Rebuilt position view for {symbol}")?;
            } else if all {
                projection.rebuild_all().await?;
                writeln!(stdout, "Rebuilt all position views")?;
            }
        }
        AggregateView::OffchainOrder => {
            let projection = Projection::<OffchainOrder>::sqlite(pool.clone());

            if let Some(raw_id) = id {
                let order_id: OffchainOrderId = raw_id.parse()?;
                projection.rebuild(&order_id).await?;
                writeln!(stdout, "Rebuilt offchain order view for {order_id}")?;
            } else if all {
                projection.rebuild_all().await?;
                writeln!(stdout, "Rebuilt all offchain order views")?;
            }
        }
        AggregateView::VaultRegistry => {
            let projection = Projection::<VaultRegistry>::sqlite(pool.clone());

            if let Some(raw_id) = id {
                let registry_id = raw_id.parse()?;
                projection.rebuild(&registry_id).await?;
                writeln!(stdout, "Rebuilt vault registry view for {raw_id}")?;
            } else if all {
                projection.rebuild_all().await?;
                writeln!(stdout, "Rebuilt all vault registry views")?;
            }
        }
        AggregateView::RebalanceTiming => {
            if id.is_some() {
                anyhow::bail!(
                    "rebalance-timing rebuild replays the whole read model; pass --all, not --id"
                );
            }

            if !all {
                anyhow::bail!("rebalance-timing rebuild replays the whole read model; pass --all");
            }

            let replayed = RebalanceTimingProjection::new(pool.clone())
                .rebuild_all()
                .await?;
            writeln!(
                stdout,
                "Rebuilt rebalance stage-timing read model ({replayed} events replayed)"
            )?;
        }
        AggregateView::LifecycleFailure => {
            if id.is_some() {
                anyhow::bail!(
                    "lifecycle-failure rebuild replays the whole read model; pass --all, not --id"
                );
            }

            if !all {
                anyhow::bail!("lifecycle-failure rebuild replays the whole read model; pass --all");
            }

            let replayed = LifecycleFailureProjection::new(pool.clone())
                .rebuild_all()
                .await?;
            writeln!(
                stdout,
                "Rebuilt lifecycle-failure read model ({replayed} events replayed)"
            )?;
        }
    }

    Ok(())
}

async fn run_position_command<W: Write>(
    stdout: &mut W,
    pool: &SqlitePool,
    command: PositionRecoveryCommand,
    execution_threshold: st0x_config::ExecutionThreshold,
) -> anyhow::Result<()> {
    match command {
        PositionRecoveryCommand::ReleaseHedge {
            symbol,
            order_id,
            reason,
        } => {
            repair::fail_pending_offchain_order_command(stdout, pool, &symbol, order_id, reason)
                .await
        }
        PositionRecoveryCommand::Set {
            symbol,
            zero,
            long,
            short,
            price,
            reason,
        } => {
            let target_net = ManualPositionTarget::from_flags(zero, long, short)?.net()?;
            repair::set_position_command(
                stdout,
                pool,
                &symbol,
                target_net,
                reason,
                execution_threshold,
                price,
            )
            .await
        }
    }
}

/// Operator-chosen target for `position set`, converted from the mutually
/// exclusive `--zero`/`--long`/`--short` clap flags at the parser boundary so
/// internal code carries one valid state instead of three flags.
enum ManualPositionTarget {
    Zero,
    Long(Positive<FractionalShares>),
    Short(Positive<FractionalShares>),
}

impl ManualPositionTarget {
    fn from_flags(
        zero: bool,
        long: Option<Positive<FractionalShares>>,
        short: Option<Positive<FractionalShares>>,
    ) -> anyhow::Result<Self> {
        match (zero, long, short) {
            (true, None, None) => Ok(Self::Zero),
            (false, Some(shares), None) => Ok(Self::Long(shares)),
            (false, None, Some(shares)) => Ok(Self::Short(shares)),
            _ => anyhow::bail!("exactly one of --zero, --long, or --short is required"),
        }
    }

    fn net(self) -> anyhow::Result<FractionalShares> {
        match self {
            Self::Zero => Ok(FractionalShares::ZERO),
            Self::Long(shares) => Ok(shares.inner()),
            Self::Short(shares) => Ok((FractionalShares::ZERO - shares.inner())?),
        }
    }
}

async fn run_provider_command<W: Write>(
    command: ProviderCommand,
    ctx: &Ctx,
    pool: &SqlitePool,
    stdout: &mut W,
    order_placer: Arc<dyn OrderPlacer>,
) -> anyhow::Result<()> {
    let provider = ProviderBuilder::new().connect_http(ctx.evm.rpc_url.clone());

    match command {
        ProviderCommand::ProcessTx { tx_hash } => {
            info!("Processing transaction: tx_hash={tx_hash}");
            let cache = SymbolCache::default();
            trading::process_tx_with_provider(
                tx_hash,
                ctx,
                pool,
                stdout,
                &provider,
                &cache,
                order_placer,
            )
            .await
        }
        ProviderCommand::TransferUsdc { direction, amount } => {
            rebalancing::transfer_usdc_command(stdout, direction, amount, ctx, pool).await
        }
        ProviderCommand::ResumeUsdcTransfer { id, direction } => {
            rebalancing::resume_usdc_transfer_command(stdout, id, direction, ctx, pool).await
        }
        ProviderCommand::CctpBridge { amount, all, from } => {
            cctp::cctp_bridge_command::<OpenChainErrorRegistry, _>(stdout, amount, all, from, ctx)
                .await
        }
        ProviderCommand::CctpRecover {
            burn_tx,
            source_chain,
        } => cctp::cctp_recover_command(stdout, burn_tx, source_chain, ctx).await,
        ProviderCommand::ResetAllowance { chain } => {
            cctp::reset_allowance_command::<OpenChainErrorRegistry, _>(stdout, chain, ctx).await
        }
        ProviderCommand::AlpacaTokenize {
            symbol,
            quantity,
            recipient,
        } => {
            rebalancing::alpaca_tokenize_command(stdout, symbol, quantity, recipient, ctx, provider)
                .await
        }
        ProviderCommand::AlpacaRedeem {
            symbol,
            quantity,
            redemption_wallet,
        } => {
            rebalancing::alpaca_redeem_command(stdout, symbol, quantity, redemption_wallet, ctx)
                .await
        }
        ProviderCommand::DividendBump { symbol, quantity } => {
            dividend::dividend_bump_command(stdout, symbol, quantity, ctx, provider).await
        }
        ProviderCommand::AlpacaTokenizationRequests => {
            rebalancing::alpaca_tokenization_requests_command(stdout, ctx).await
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash, address};
    use chrono::Utc;
    use clap::{CommandFactory, Parser};
    use url::Url;

    use st0x_config::ExecutionThreshold;
    use st0x_config::create_test_issuance_ctx;
    use st0x_config::{AssetsConfig, BrokerCtx, EquitiesConfig, LogLevel, TradingMode};
    use st0x_config::{EvmCtx, IngestionCutoff};

    use super::*;
    use crate::offchain::order::OffchainOrderEvent;
    use crate::test_utils::{positive_shares, setup_test_db};

    fn create_test_ctx() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            log_dir: None,
            server_port: 8080,
            board_port: 8081,
            evm: EvmCtx {
                rpc_url: Url::parse("http://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                deployment_block: 1,
                required_confirmations: 0,
                ingestion_cutoff: IngestionCutoff::Safe,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            order_fill_poll_interval: 5,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            alerts: None,
            trading_mode: TradingMode::Standalone,
            order_owner: Address::ZERO,
            wallet: None,
            wallet_meta: None,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
            travel_rule: None,
            rest_api: None,
            issuance: create_test_issuance_ctx(),
            redemption_wallet: None,
        }
    }

    #[test]
    fn cli_uses_updated_binary_name() {
        let command = Cli::command();
        assert_eq!(command.get_name(), "st0x-cli");
    }

    #[test]
    fn buy_command_parses_fractional_quantity() {
        let cli = Cli::try_parse_from(["st0x-cli", "buy", "-s", "SPYM", "-q", "6.15"]).unwrap();

        match cli.command {
            Commands::Buy {
                symbol, quantity, ..
            } => {
                assert_eq!(symbol, Symbol::new("SPYM").unwrap());
                assert_eq!(quantity, positive_shares("6.15"));
            }
            other => panic!("expected buy command, got: {other:?}"),
        }
    }

    #[test]
    fn dividend_bump_command_parses_symbol_and_quantity() {
        let cli =
            Cli::try_parse_from(["st0x-cli", "dividend-bump", "-s", "COIN", "-q", "10.5"]).unwrap();

        match cli.command {
            Commands::DividendBump { symbol, quantity } => {
                assert_eq!(symbol, Symbol::new("COIN").unwrap());
                assert_eq!(quantity, positive_shares("10.5"));
            }
            other => panic!("expected dividend-bump command, got: {other:?}"),
        }
    }

    #[test]
    fn dividend_bump_command_rejects_zero_quantity() {
        let error = Cli::try_parse_from(["st0x-cli", "dividend-bump", "-s", "COIN", "-q", "0"])
            .unwrap_err();
        let rendered = error.to_string();
        assert!(rendered.contains('0'), "unexpected clap error: {rendered}");
    }

    #[test]
    fn buy_command_rejects_zero_quantity() {
        let error = Cli::try_parse_from(["st0x-cli", "buy", "-s", "AAPL", "-q", "0"]).unwrap_err();
        let rendered = error.to_string();
        assert!(rendered.contains('0'), "unexpected clap error: {rendered}");
    }

    #[test]
    fn sell_command_rejects_zero_quantity() {
        let error = Cli::try_parse_from(["st0x-cli", "sell", "-s", "AAPL", "-q", "0"]).unwrap_err();
        let rendered = error.to_string();
        assert!(rendered.contains('0'), "unexpected clap error: {rendered}");
    }

    #[test]
    fn wrap_equity_command_rejects_zero_quantity() {
        let error =
            Cli::try_parse_from(["st0x-cli", "wrap-equity", "-s", "AAPL", "-q", "0"]).unwrap_err();
        let rendered = error.to_string();
        assert!(rendered.contains('0'), "unexpected clap error: {rendered}");
    }

    #[test]
    fn unwrap_equity_command_rejects_zero_quantity() {
        let error = Cli::try_parse_from(["st0x-cli", "unwrap-equity", "-s", "AAPL", "-q", "0"])
            .unwrap_err();
        let rendered = error.to_string();
        assert!(rendered.contains('0'), "unexpected clap error: {rendered}");
    }

    #[test]
    fn sell_command_parses_fractional_quantity() {
        let cli = Cli::try_parse_from(["st0x-cli", "sell", "-s", "SPYM", "-q", "6.15"]).unwrap();

        match cli.command {
            Commands::Sell {
                symbol, quantity, ..
            } => {
                assert_eq!(symbol, Symbol::new("SPYM").unwrap());
                assert_eq!(quantity, positive_shares("6.15"));
            }
            other => panic!("expected sell command, got: {other:?}"),
        }
    }

    #[test]
    fn wrap_equity_command_parses_fractional_quantity() {
        let cli =
            Cli::try_parse_from(["st0x-cli", "wrap-equity", "-s", "SPYM", "-q", "6.15"]).unwrap();

        match cli.command {
            Commands::WrapEquity { symbol, quantity } => {
                assert_eq!(symbol, Symbol::new("SPYM").unwrap());
                assert_eq!(quantity, positive_shares("6.15"));
            }
            other => panic!("expected wrap-equity command, got: {other:?}"),
        }
    }

    #[test]
    fn parse_positive_shares_rejects_zero() {
        let error = parse_positive_shares("0").unwrap_err();
        assert!(error.contains("positive"), "unexpected error: {error}");
    }

    #[test]
    fn classify_buy_command_as_simple() {
        let command = Commands::Buy {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: positive_shares("1"),
            time_in_force: None,
            limit_price: None,
            extended_hours: false,
        };

        match classify_command(command) {
            Ok(SimpleCommand::Buy { .. }) => {}
            Ok(_) => panic!("expected buy simple command"),
            Err(
                ProviderCommand::ProcessTx { .. }
                | ProviderCommand::TransferUsdc { .. }
                | ProviderCommand::ResumeUsdcTransfer { .. }
                | ProviderCommand::CctpBridge { .. }
                | ProviderCommand::CctpRecover { .. }
                | ProviderCommand::ResetAllowance { .. }
                | ProviderCommand::AlpacaTokenize { .. }
                | ProviderCommand::AlpacaRedeem { .. }
                | ProviderCommand::DividendBump { .. }
                | ProviderCommand::AlpacaTokenizationRequests,
            ) => panic!("expected simple command classification"),
        }
    }

    #[test]
    fn classify_process_tx_command_as_provider() {
        let command = Commands::ProcessTx {
            tx_hash: TxHash::ZERO,
        };

        match classify_command(command) {
            Err(ProviderCommand::ProcessTx { .. }) => {}
            Err(
                ProviderCommand::TransferUsdc { .. }
                | ProviderCommand::ResumeUsdcTransfer { .. }
                | ProviderCommand::CctpBridge { .. }
                | ProviderCommand::CctpRecover { .. }
                | ProviderCommand::ResetAllowance { .. }
                | ProviderCommand::AlpacaTokenize { .. }
                | ProviderCommand::AlpacaRedeem { .. }
                | ProviderCommand::DividendBump { .. }
                | ProviderCommand::AlpacaTokenizationRequests,
            ) => panic!("expected process-tx provider command"),
            Ok(_) => panic!("expected provider command classification"),
        }
    }

    #[test]
    fn classify_fail_usdc_transfer_routes_without_provider() {
        // fail-usdc-transfer only loads + sends to the event store, so it
        // must route without an RPC provider.
        let command = Commands::FailUsdcTransfer {
            id: Uuid::from_u128(123),
            reason: "test reason".to_string(),
        };

        match classify_command(command) {
            Ok(SimpleCommand::FailUsdcTransfer { .. }) => {}
            Ok(_) => panic!("expected fail-usdc-transfer simple command"),
            Err(
                ProviderCommand::ProcessTx { .. }
                | ProviderCommand::TransferUsdc { .. }
                | ProviderCommand::ResumeUsdcTransfer { .. }
                | ProviderCommand::CctpBridge { .. }
                | ProviderCommand::CctpRecover { .. }
                | ProviderCommand::ResetAllowance { .. }
                | ProviderCommand::AlpacaTokenize { .. }
                | ProviderCommand::AlpacaRedeem { .. }
                | ProviderCommand::DividendBump { .. }
                | ProviderCommand::AlpacaTokenizationRequests,
            ) => panic!("expected simple command classification, got provider command"),
        }
    }

    #[test]
    fn fail_usdc_transfer_parses() {
        let id = Uuid::from_u128(0xAB_CD_EF);
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "fail-usdc-transfer",
            "--id",
            &id.to_string(),
            "--reason",
            "stuck pre-burn",
        ])
        .unwrap();

        match cli.command {
            Commands::FailUsdcTransfer {
                id: parsed_id,
                reason,
            } => {
                assert_eq!(parsed_id, id);
                assert_eq!(reason, "stuck pre-burn");
            }
            other => panic!("expected fail-usdc-transfer command, got: {other:?}"),
        }
    }

    #[test]
    fn fail_usdc_transfer_uses_default_reason_when_omitted() {
        let id = Uuid::from_u128(0xDE_AD);
        let cli = Cli::try_parse_from(["st0x-cli", "fail-usdc-transfer", "--id", &id.to_string()])
            .unwrap();

        match cli.command {
            Commands::FailUsdcTransfer { reason, .. } => {
                assert_eq!(
                    reason, "Manually failed via CLI",
                    "omitting --reason must use the default"
                );
            }
            other => panic!("expected fail-usdc-transfer command, got: {other:?}"),
        }
    }

    #[test]
    fn transfer_resume_requires_kind() {
        // `--kind` is intentionally not defaulted (so required_if_eq can enforce
        // the usdc dependency), so a bare `transfer resume` must error.
        let error = Cli::try_parse_from(["st0x-cli", "transfer", "resume"]).unwrap_err();
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn transfer_fail_requires_reason() {
        // Destructive verbs persist `--reason` as the audit record, so the new
        // grouped name must require it (no default).
        let error = Cli::try_parse_from([
            "st0x-cli", "transfer", "fail", "--kind", "mint", "--id", "m1",
        ])
        .unwrap_err();
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
        assert!(
            error.to_string().contains("reason"),
            "the missing argument must be --reason, got: {error}",
        );
    }

    #[test]
    fn transfer_reconcile_requires_reason() {
        // Supply --kind and --id so the only missing required arg is --reason,
        // isolating the audit-record requirement from the other required flags.
        let id = Uuid::from_u128(7);
        let error = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "reconcile",
            "--kind",
            "usdc",
            "--id",
            &id.to_string(),
        ])
        .unwrap_err();
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
        assert!(
            error.to_string().contains("reason"),
            "the missing argument must be --reason, got: {error}",
        );
    }

    #[test]
    fn classify_clear_pending_burn_routes_without_provider() {
        // clear-pending-burn only loads + sends to the event store, so it
        // must route without an RPC provider.
        let command = Commands::ClearPendingBurn {
            id: Uuid::from_u128(456),
            reason: "test reason".to_string(),
        };

        match classify_command(command) {
            Ok(SimpleCommand::ClearPendingBurn { .. }) => {}
            Ok(_) => panic!("expected clear-pending-burn simple command"),
            Err(
                ProviderCommand::ProcessTx { .. }
                | ProviderCommand::TransferUsdc { .. }
                | ProviderCommand::ResumeUsdcTransfer { .. }
                | ProviderCommand::CctpBridge { .. }
                | ProviderCommand::CctpRecover { .. }
                | ProviderCommand::ResetAllowance { .. }
                | ProviderCommand::AlpacaTokenize { .. }
                | ProviderCommand::AlpacaRedeem { .. }
                | ProviderCommand::DividendBump { .. }
                | ProviderCommand::AlpacaTokenizationRequests,
            ) => panic!("expected simple command classification, got provider command"),
        }
    }

    #[test]
    fn clear_pending_burn_parses() {
        let id = Uuid::from_u128(0x12_34_56);
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "clear-pending-burn",
            "--id",
            &id.to_string(),
            "--reason",
            "burn verified absent",
        ])
        .unwrap();

        match cli.command {
            Commands::ClearPendingBurn {
                id: parsed_id,
                reason,
            } => {
                assert_eq!(parsed_id, id);
                assert_eq!(reason, "burn verified absent");
            }
            other => panic!("expected clear-pending-burn command, got: {other:?}"),
        }
    }

    #[test]
    fn clear_pending_burn_uses_default_reason_when_omitted() {
        let id = Uuid::from_u128(0xFE_ED);
        let cli = Cli::try_parse_from(["st0x-cli", "clear-pending-burn", "--id", &id.to_string()])
            .unwrap();

        match cli.command {
            Commands::ClearPendingBurn { reason, .. } => {
                assert_eq!(
                    reason, "Burn verified absent on-chain via CLI",
                    "omitting --reason must use the default"
                );
            }
            other => panic!("expected clear-pending-burn command, got: {other:?}"),
        }
    }

    #[test]
    fn position_release_hedge_requires_reason() {
        let order_id = OffchainOrderId::new();
        let error = Cli::try_parse_from([
            "st0x-cli",
            "position",
            "release-hedge",
            "--symbol",
            "MSTR",
            "--order-id",
            &order_id.to_string(),
        ])
        .unwrap_err();
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
        assert!(
            error.to_string().contains("reason"),
            "the missing argument must be --reason, got: {error}",
        );
    }

    #[test]
    fn transfer_fail_rejects_blank_reason() {
        // A present-but-empty --reason persists the same audit-hostile blank
        // reason the requirement exists to prevent.
        let error = Cli::try_parse_from([
            "st0x-cli", "transfer", "fail", "--kind", "mint", "--id", "m1", "--reason", "   ",
        ])
        .unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
        assert!(
            error.to_string().contains("must not be blank"),
            "unexpected error: {error}",
        );
    }

    #[test]
    fn position_set_rejects_blank_reason() {
        let error = Cli::try_parse_from([
            "st0x-cli", "position", "set", "--symbol", "MSTR", "--zero", "--reason", "   ",
        ])
        .unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
        assert!(
            error.to_string().contains("must not be blank"),
            "unexpected error: {error}",
        );
    }

    #[test]
    fn position_release_hedge_rejects_blank_reason() {
        let order_id = OffchainOrderId::new();
        let error = Cli::try_parse_from([
            "st0x-cli",
            "position",
            "release-hedge",
            "--symbol",
            "MSTR",
            "--order-id",
            &order_id.to_string(),
            "--reason",
            "",
        ])
        .unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
        assert!(
            error.to_string().contains("must not be blank"),
            "unexpected error: {error}",
        );
    }

    #[test]
    fn position_set_zero_parses() {
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "position",
            "set",
            "--symbol",
            "SPYM",
            "--zero",
            "--reason",
            "manual rebalance completed",
        ])
        .unwrap();

        match classify_command(cli.command) {
            Ok(SimpleCommand::Position {
                command:
                    PositionRecoveryCommand::Set {
                        symbol,
                        zero,
                        long,
                        short,
                        price,
                        reason,
                    },
            }) => {
                assert_eq!(symbol, Symbol::new("SPYM").unwrap());
                assert!(zero);
                assert_eq!(long, None);
                assert_eq!(short, None);
                assert!(price.is_none());
                assert_eq!(reason, "manual rebalance completed");
            }
            _ => panic!("expected position set simple command"),
        }
    }

    #[test]
    fn position_set_long_and_short_parse_positive_amounts() {
        let long = Cli::try_parse_from([
            "st0x-cli",
            "position",
            "set",
            "--symbol",
            "SPYM",
            "--long",
            "100",
            "--reason",
            "manual buy",
        ])
        .unwrap();

        match classify_command(long.command) {
            Ok(SimpleCommand::Position {
                command:
                    PositionRecoveryCommand::Set {
                        long: Some(quantity),
                        ..
                    },
            }) => assert_eq!(quantity, positive_shares("100")),
            _ => panic!("expected position set --long simple command"),
        }

        let short = Cli::try_parse_from([
            "st0x-cli",
            "position",
            "set",
            "--symbol",
            "SPYM",
            "--short",
            "12.5",
            "--reason",
            "manual sell",
        ])
        .unwrap();

        match classify_command(short.command) {
            Ok(SimpleCommand::Position {
                command:
                    PositionRecoveryCommand::Set {
                        short: Some(quantity),
                        ..
                    },
            }) => assert_eq!(quantity, positive_shares("12.5")),
            _ => panic!("expected position set --short simple command"),
        }
    }

    #[test]
    fn position_set_rejects_missing_reason() {
        let error =
            Cli::try_parse_from(["st0x-cli", "position", "set", "--symbol", "SPYM", "--zero"])
                .unwrap_err();
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
        let rendered = error.to_string();
        assert!(
            rendered.contains("reason"),
            "unexpected clap error: {rendered}"
        );
    }

    #[test]
    fn position_set_rejects_multiple_targets() {
        let error = Cli::try_parse_from([
            "st0x-cli",
            "position",
            "set",
            "--symbol",
            "SPYM",
            "--zero",
            "--long",
            "1",
            "--reason",
            "operator correction",
        ])
        .unwrap_err();
        let rendered = error.to_string();
        assert!(
            rendered.contains("cannot be used with")
                && rendered.contains("--zero")
                && rendered.contains("--long"),
            "unexpected clap error: {rendered}"
        );
    }

    #[test]
    fn position_set_rejects_zero_long_amount() {
        let error = Cli::try_parse_from([
            "st0x-cli",
            "position",
            "set",
            "--symbol",
            "SPYM",
            "--long",
            "0",
            "--reason",
            "operator correction",
        ])
        .unwrap_err();
        let rendered = error.to_string();
        assert!(
            rendered.contains("invalid value '0'")
                && rendered.contains("--long")
                && rendered.contains("value must be positive"),
            "unexpected clap error: {rendered}"
        );
    }

    #[test]
    fn classify_position_command_as_simple() {
        let order_id = OffchainOrderId::new();
        let command = Commands::Position {
            command: PositionRecoveryCommand::ReleaseHedge {
                symbol: Symbol::new("MSTR").unwrap(),
                order_id,
                reason: "operator repair".to_string(),
            },
        };

        match classify_command(command) {
            Ok(SimpleCommand::Position {
                command:
                    PositionRecoveryCommand::ReleaseHedge {
                        symbol,
                        order_id: parsed_order_id,
                        reason,
                    },
            }) => {
                assert_eq!(symbol, Symbol::new("MSTR").unwrap());
                assert_eq!(parsed_order_id, order_id);
                assert_eq!(reason, "operator repair");
            }
            Ok(_) => panic!("expected position simple command"),
            Err(
                ProviderCommand::ProcessTx { .. }
                | ProviderCommand::TransferUsdc { .. }
                | ProviderCommand::ResumeUsdcTransfer { .. }
                | ProviderCommand::CctpBridge { .. }
                | ProviderCommand::CctpRecover { .. }
                | ProviderCommand::ResetAllowance { .. }
                | ProviderCommand::AlpacaTokenize { .. }
                | ProviderCommand::AlpacaRedeem { .. }
                | ProviderCommand::DividendBump { .. }
                | ProviderCommand::AlpacaTokenizationRequests,
            ) => panic!("expected simple command classification"),
        }
    }

    #[tokio::test]
    async fn run_command_with_writers_executes_position_set() {
        let ctx = create_test_ctx();
        let pool = setup_test_db().await;
        let symbol = Symbol::new("SPYM").unwrap();
        let command = Commands::Position {
            command: PositionRecoveryCommand::Set {
                symbol: symbol.clone(),
                zero: false,
                long: Some(positive_shares("100")),
                short: None,
                price: None,
                reason: "manual buy not observed by bot".to_string(),
            },
        };

        let mut stdout_buffer = Vec::new();
        run_command_with_writers(ctx, command, &pool, &mut stdout_buffer)
            .await
            .unwrap();

        let projection = Projection::<Position>::sqlite(pool.clone());
        let view = projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(view.net, positive_shares("100").inner());

        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("Set SPYM position from 0 to 100"),
            "unexpected output: {output}"
        );
    }

    #[tokio::test]
    async fn rebuild_view_rebalance_timing_requires_all() {
        let pool = setup_test_db().await;
        let mut stdout_buffer = Vec::new();

        let error = rebuild_view(
            &mut stdout_buffer,
            &pool,
            AggregateView::RebalanceTiming,
            None,
            false,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "rebalance-timing rebuild replays the whole read model; pass --all"
        );
    }

    #[tokio::test]
    async fn rebuild_view_rebalance_timing_rejects_id() {
        let pool = setup_test_db().await;
        let mut stdout_buffer = Vec::new();

        let error = rebuild_view(
            &mut stdout_buffer,
            &pool,
            AggregateView::RebalanceTiming,
            Some("AAPL".to_string()),
            false,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "rebalance-timing rebuild replays the whole read model; pass --all, not --id"
        );
    }

    #[tokio::test]
    async fn rebuild_view_lifecycle_failure_requires_all() {
        let pool = setup_test_db().await;
        let mut stdout_buffer = Vec::new();

        let error = rebuild_view(
            &mut stdout_buffer,
            &pool,
            AggregateView::LifecycleFailure,
            None,
            false,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "lifecycle-failure rebuild replays the whole read model; pass --all"
        );
    }

    #[tokio::test]
    async fn rebuild_view_lifecycle_failure_rejects_id() {
        let pool = setup_test_db().await;
        let mut stdout_buffer = Vec::new();

        let error = rebuild_view(
            &mut stdout_buffer,
            &pool,
            AggregateView::LifecycleFailure,
            Some("AAPL".to_string()),
            false,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "lifecycle-failure rebuild replays the whole read model; pass --all, not --id"
        );
    }

    #[tokio::test]
    async fn rebuild_view_lifecycle_failure_rebuilds_from_event_log() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();

        // Seed a real OffchainOrder failure event so the rebuild has something to
        // fold. The replay path deserializes events.payload, so it must be a
        // genuine serialized OffchainOrderEvent, not a hand-written shape.
        let failed = OffchainOrderEvent::Failed {
            error: "rejected".to_string(),
            failed_at: Utc::now(),
        };
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES ('OffchainOrder', ?, 1, 'OffchainOrderEvent::Failed', '1', ?, '{}')",
        )
        .bind(order_id.to_string())
        .bind(serde_json::to_string(&failed).unwrap())
        .execute(&pool)
        .await
        .unwrap();

        let mut stdout_buffer = Vec::new();
        rebuild_view(
            &mut stdout_buffer,
            &pool,
            AggregateView::LifecycleFailure,
            None,
            true,
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("Rebuilt lifecycle-failure read model (1 events replayed)"),
            "unexpected rebuild output: {output}"
        );

        let failure_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM lifecycle_failure_event")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            failure_rows, 1,
            "the rebuild folded the one seeded failure event into the read model"
        );
    }

    #[test]
    fn manual_position_target_converts_short_to_negative_net() {
        let target = ManualPositionTarget::from_flags(false, None, Some(positive_shares("12.5")))
            .unwrap()
            .net()
            .unwrap();
        let expected = (FractionalShares::ZERO - positive_shares("12.5").inner()).unwrap();

        assert_eq!(target, expected);
    }

    #[test]
    fn transfer_resume_parses_and_classifies_as_resume_provider() {
        let id = Uuid::from_u128(42);
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "resume",
            "--kind",
            "usdc",
            "--id",
            &id.to_string(),
            "--direction",
            "to-raindex",
        ])
        .unwrap();

        match classify_command(cli.command) {
            Err(ProviderCommand::ResumeUsdcTransfer {
                id: parsed_id,
                direction,
            }) => {
                assert_eq!(parsed_id, id);
                assert!(matches!(direction, TransferDirection::ToRaindex));
            }
            _ => panic!("expected resume provider command"),
        }
    }

    #[test]
    fn transfer_resume_usdc_to_alpaca_classifies_with_to_alpaca_direction() {
        let id = Uuid::from_u128(43);
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "resume",
            "--kind",
            "usdc",
            "--id",
            &id.to_string(),
            "--direction",
            "to-alpaca",
        ])
        .unwrap();

        match classify_command(cli.command) {
            Err(ProviderCommand::ResumeUsdcTransfer {
                id: parsed_id,
                direction,
            }) => {
                assert_eq!(parsed_id, id);
                assert!(matches!(direction, TransferDirection::ToAlpaca));
            }
            _ => panic!("expected resume provider command"),
        }
    }

    #[test]
    fn transfer_resume_usdc_rejects_removed_amount_flag() {
        let id = Uuid::from_u128(44);
        let error = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "resume",
            "--kind",
            "usdc",
            "--id",
            &id.to_string(),
            "--direction",
            "to-raindex",
            "--amount",
            "100",
        ])
        .unwrap_err();

        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::UnknownArgument,
            "the removed --amount flag must no longer parse; got: {error}",
        );
    }

    #[test]
    #[should_panic(expected = "guarantees id and direction")]
    fn classify_usdc_resume_without_id_or_direction_is_unreachable() {
        // The production path is doubly guarded (clap `required_if_eq` plus
        // `validate_command`), but `classify_command` is called directly in
        // tests; constructing the impossible shape by hand must hit the
        // documented `unreachable!`, pinning that invariant independently.
        let command = Commands::Transfer {
            command: TransferCommand::Resume {
                kind: TransferResumeKind::Usdc,
                id: None,
                direction: None,
            },
        };

        let _ = classify_command(command);
    }

    #[test]
    fn transfer_reconcile_usdc_parses_and_classifies_as_simple() {
        let id = Uuid::from_u128(77);
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "reconcile",
            "--kind",
            "usdc",
            "--id",
            &id.to_string(),
            "--reason",
            "deposit-credited-offline",
        ])
        .unwrap();

        match classify_command(cli.command) {
            Ok(SimpleCommand::ReconcileUsdcTransfer {
                id: parsed_id,
                reason,
            }) => {
                assert_eq!(parsed_id, id.to_string());
                assert_eq!(reason, "deposit-credited-offline");
            }
            _ => panic!("expected reconcile usdc simple command"),
        }
    }

    #[test]
    fn transfer_reconcile_mint_parses_and_classifies_as_equity() {
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "reconcile",
            "--kind",
            "mint",
            "--id",
            "ISS001",
            "--reason",
            "wrapped manually via wrap-equity",
        ])
        .unwrap();

        match classify_command(cli.command) {
            Ok(SimpleCommand::ReconcileEquityTransfer {
                transfer_type,
                id,
                reason,
            }) => {
                assert!(matches!(transfer_type, TransferType::Mint));
                assert_eq!(id, "ISS001");
                assert_eq!(reason, "wrapped manually via wrap-equity");
            }
            _ => panic!("expected reconcile equity (mint) simple command"),
        }
    }

    #[test]
    fn transfer_reconcile_redemption_parses_and_classifies_as_equity() {
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "reconcile",
            "--kind",
            "redemption",
            "--id",
            "RED-001",
            "--reason",
            "deposited manually",
        ])
        .unwrap();

        match classify_command(cli.command) {
            Ok(SimpleCommand::ReconcileEquityTransfer {
                transfer_type,
                id,
                reason,
            }) => {
                assert!(matches!(transfer_type, TransferType::Redemption));
                assert_eq!(id, "RED-001");
                assert_eq!(reason, "deposited manually");
            }
            _ => panic!("expected reconcile equity (redemption) simple command"),
        }
    }

    #[test]
    fn transfer_reconcile_usdc_rejects_unknown_reason() {
        let error = parse_usdc_reconcile_reason("bogus-reason").unwrap_err();
        assert!(
            error.to_string().contains("unknown --reason"),
            "unknown usdc reconcile reason must surface a clear error, got: {error}"
        );
    }

    #[test]
    fn transfer_reconcile_rejects_blank_reason() {
        // `OperatorReconciled { reason }` is the permanent audit record; a
        // present-but-blank reason must be rejected at parse time for every
        // kind, not just usdc (whose enum domain rejects it).
        for kind in ["usdc", "mint", "redemption"] {
            let error = Cli::try_parse_from([
                "st0x-cli",
                "transfer",
                "reconcile",
                "--kind",
                kind,
                "--id",
                "x",
                "--reason",
                "   ",
            ])
            .unwrap_err();
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::ValueValidation,
                "blank --reason must be rejected for --kind {kind}",
            );
            assert!(
                error.to_string().contains("must not be blank"),
                "unexpected error for --kind {kind}: {error}",
            );
        }
    }

    #[test]
    fn transfer_fail_parses_and_classifies_as_simple() {
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "fail",
            "--kind",
            "mint",
            "--id",
            "ISS001",
            "--reason",
            "stuck forever",
        ])
        .unwrap();

        match classify_command(cli.command) {
            Ok(SimpleCommand::FailTransfer {
                transfer_type,
                id,
                reason,
            }) => {
                assert!(matches!(transfer_type, TransferType::Mint));
                assert_eq!(id, "ISS001");
                assert_eq!(reason, "stuck forever");
            }
            _ => panic!("expected transfer fail simple command"),
        }
    }

    #[test]
    fn transfer_recheck_parses_and_classifies_as_simple() {
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "recheck",
            "--kind",
            "redemption",
            "--id",
            "redemption-1",
        ])
        .unwrap();

        match classify_command(cli.command) {
            Ok(SimpleCommand::RecheckTransfer { transfer_type, id }) => {
                assert!(matches!(transfer_type, TransferType::Redemption));
                assert_eq!(id, "redemption-1");
            }
            _ => panic!("expected recheck simple command"),
        }
    }

    #[test]
    fn view_rebuild_parses_and_classifies_as_simple() {
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "view",
            "rebuild",
            "--aggregate",
            "position",
            "--id",
            "AAPL",
        ])
        .unwrap();

        match classify_command(cli.command) {
            Ok(SimpleCommand::RebuildView { aggregate, id, all }) => {
                assert!(matches!(aggregate, AggregateView::Position));
                assert_eq!(id.as_deref(), Some("AAPL"));
                assert!(!all);
            }
            _ => panic!("expected view rebuild simple command"),
        }
    }

    #[test]
    fn cctp_complete_mint_parses_and_classifies_as_provider() {
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "cctp",
            "complete-mint",
            "--burn-tx",
            &TxHash::ZERO.to_string(),
            "--source-chain",
            "ethereum",
        ])
        .unwrap();

        match classify_command(cli.command) {
            Err(ProviderCommand::CctpRecover {
                burn_tx,
                source_chain,
            }) => {
                assert_eq!(burn_tx, TxHash::ZERO);
                assert!(matches!(source_chain, CctpChain::Ethereum));
            }
            _ => panic!("expected cctp complete-mint provider command"),
        }
    }

    #[test]
    fn position_release_hedge_parses_under_new_name() {
        let order_id = OffchainOrderId::new();
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "position",
            "release-hedge",
            "--symbol",
            "MSTR",
            "--order-id",
            &order_id.to_string(),
            "--reason",
            "operator repair",
        ])
        .unwrap();

        match classify_command(cli.command) {
            Ok(SimpleCommand::Position {
                command:
                    PositionRecoveryCommand::ReleaseHedge {
                        symbol,
                        order_id: parsed_order_id,
                        reason,
                    },
            }) => {
                assert_eq!(symbol, Symbol::new("MSTR").unwrap());
                assert_eq!(parsed_order_id, order_id);
                assert_eq!(
                    reason, "operator repair",
                    "the supplied --reason must survive classify_command",
                );
            }
            _ => panic!("expected position release-hedge to classify as a simple position command"),
        }
    }

    /// A supplied `--id`/`--direction` with `--kind equity` signals the
    /// operator probably meant `--kind usdc`; it must be rejected, never
    /// silently discarded.
    #[test]
    fn validate_rejects_equity_resume_with_id() {
        let with_id = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "resume",
            "--kind",
            "equity",
            "--id",
            &Uuid::from_u128(7).to_string(),
        ])
        .unwrap();

        let error = validate_command(&with_id.command).unwrap_err();
        assert!(
            error.to_string().contains("--kind usdc"),
            "the rejection must point at --kind usdc; got: {error}"
        );
    }

    /// Both flags at once must also be rejected.
    #[test]
    fn validate_rejects_equity_resume_with_both_flags() {
        let with_both = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "resume",
            "--kind",
            "equity",
            "--id",
            &Uuid::from_u128(7).to_string(),
            "--direction",
            "to-raindex",
        ])
        .unwrap();

        let error = validate_command(&with_both.command).unwrap_err();
        assert!(
            error.to_string().contains("takes no --id/--direction"),
            "got: {error}"
        );
    }

    /// Same for a supplied `--direction`.
    #[test]
    fn validate_rejects_equity_resume_with_direction() {
        let with_direction = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "resume",
            "--kind",
            "equity",
            "--direction",
            "to-raindex",
        ])
        .unwrap();

        let error = validate_command(&with_direction.command).unwrap_err();
        assert!(
            error.to_string().contains("takes no --id/--direction"),
            "got: {error}"
        );
    }

    /// A bare `--kind equity` passes validation and routes to the bulk
    /// equity resume.
    #[test]
    fn validate_accepts_bare_equity_resume() {
        let clean =
            Cli::try_parse_from(["st0x-cli", "transfer", "resume", "--kind", "equity"]).unwrap();
        validate_command(&clean.command).unwrap();
        assert!(matches!(
            classify_command(clean.command),
            Ok(SimpleCommand::ResumeInterruptedTransfers)
        ));
    }

    /// The defensive Usdc guard exists for callers that bypass clap (the only
    /// path where it can fire); construct the bad shape directly.
    #[test]
    fn validate_rejects_directly_constructed_usdc_resume_without_args() {
        let command = Commands::Transfer {
            command: TransferCommand::Resume {
                kind: TransferResumeKind::Usdc,
                id: None,
                direction: None,
            },
        };

        let error = validate_command(&command).unwrap_err();
        assert!(
            error.to_string().contains("requires --id and --direction"),
            "got: {error}"
        );
    }

    /// The two `required_if_eq("kind","usdc")` annotations are independent:
    /// each missing arg must be rejected at parse time on its own, or the
    /// classify `unreachable!` guarantee silently rots.
    #[test]
    fn usdc_resume_requires_each_arg_independently() {
        let missing_direction = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "resume",
            "--kind",
            "usdc",
            "--id",
            &Uuid::from_u128(7).to_string(),
        ])
        .unwrap_err();
        assert_eq!(
            missing_direction.kind(),
            clap::error::ErrorKind::MissingRequiredArgument,
            "--kind usdc without --direction must be rejected at parse time",
        );

        let missing_id = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "resume",
            "--kind",
            "usdc",
            "--direction",
            "to-raindex",
        ])
        .unwrap_err();
        assert_eq!(
            missing_id.kind(),
            clap::error::ErrorKind::MissingRequiredArgument,
            "--kind usdc without --id must be rejected at parse time",
        );
    }

    /// The legacy `--type`/`-t` discriminator must keep working on the new
    /// grouped `transfer fail`, so mechanically translated runbook invocations
    /// parse and carry the right `TransferType`.
    #[test]
    fn transfer_fail_accepts_type_alias() {
        let long = Cli::try_parse_from([
            "st0x-cli", "transfer", "fail", "--type", "mint", "--id", "ISS001", "--reason", "stuck",
        ])
        .unwrap();
        match classify_command(long.command) {
            Ok(SimpleCommand::FailTransfer { transfer_type, .. }) => {
                assert!(matches!(transfer_type, TransferType::Mint));
            }
            _ => panic!("expected transfer fail simple command via --type"),
        }

        let short = Cli::try_parse_from([
            "st0x-cli", "transfer", "fail", "-t", "mint", "--id", "ISS001", "-r", "stuck",
        ])
        .unwrap();
        match classify_command(short.command) {
            Ok(SimpleCommand::FailTransfer { transfer_type, .. }) => {
                assert!(matches!(transfer_type, TransferType::Mint));
            }
            _ => panic!("expected transfer fail simple command via -t"),
        }
    }

    /// The legacy `--type`/`-t` discriminator must keep working on the new
    /// grouped `transfer recheck`, so mechanically translated runbook
    /// invocations parse and carry the right `TransferType`.
    #[test]
    fn transfer_recheck_accepts_type_alias() {
        let long = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "recheck",
            "--type",
            "redemption",
            "--id",
            "redemption-1",
        ])
        .unwrap();
        match classify_command(long.command) {
            Ok(SimpleCommand::RecheckTransfer { transfer_type, .. }) => {
                assert!(matches!(transfer_type, TransferType::Redemption));
            }
            _ => panic!("expected transfer recheck simple command via --type"),
        }

        let short = Cli::try_parse_from([
            "st0x-cli",
            "transfer",
            "recheck",
            "-t",
            "redemption",
            "--id",
            "redemption-1",
        ])
        .unwrap();
        match classify_command(short.command) {
            Ok(SimpleCommand::RecheckTransfer { transfer_type, .. }) => {
                assert!(matches!(transfer_type, TransferType::Redemption));
            }
            _ => panic!("expected transfer recheck simple command via -t"),
        }
    }

    /// The legacy recovery names were removed (no back-compat). Pin that each
    /// removed flat command and clap alias now fails to parse, so a future
    /// merge cannot silently re-introduce one. One representative per removed
    /// namespace: flat top-level commands, the `repair` group alias, and the
    /// `set-position`/`fail-pending-offchain-order` subcommand aliases.
    #[test]
    fn removed_legacy_recovery_names_no_longer_parse() {
        for argv in [
            ["st0x-cli", "fail-transfer", "--id", "ISS001"].as_slice(),
            ["st0x-cli", "recheck-transfer", "--id", "ISS001"].as_slice(),
            ["st0x-cli", "resume-usdc-transfer", "--id", "x"].as_slice(),
            ["st0x-cli", "reconcile-usdc-transfer", "--id", "x"].as_slice(),
            ["st0x-cli", "rebuild-view", "--all"].as_slice(),
            ["st0x-cli", "cctp-recover", "--burn-tx", "0x0"].as_slice(),
            ["st0x-cli", "repair", "set-position", "--symbol", "SPYM"].as_slice(),
            ["st0x-cli", "position", "set-position", "--symbol", "SPYM"].as_slice(),
            [
                "st0x-cli",
                "position",
                "fail-pending-offchain-order",
                "--symbol",
                "MSTR",
                "--order-id",
                "00000000-0000-0000-0000-000000000000",
            ]
            .as_slice(),
        ] {
            let error = Cli::try_parse_from(argv.iter().copied()).unwrap_err();
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::InvalidSubcommand,
                "removed name still parses: {argv:?}",
            );
        }
    }

    #[tokio::test]
    async fn run_command_with_writers_executes_dry_run_buy() {
        let ctx = create_test_ctx();
        let pool = setup_test_db().await;
        let command = Commands::Buy {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: positive_shares("1"),
            time_in_force: None,
            limit_price: None,
            extended_hours: false,
        };

        let mut stdout_buffer = Vec::new();
        let () = run_command_with_writers(ctx, command, &pool, &mut stdout_buffer)
            .await
            .unwrap();

        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("Order placed successfully"),
            "unexpected output: {output}"
        );
    }

    #[test]
    fn extended_hours_without_limit_price_is_rejected_by_request_builder() {
        let result = trading::CliOrderRequest::from_cli_args(
            Symbol::new("AAPL").unwrap(),
            positive_shares("1"),
            Direction::Buy,
            None,
            None,
            true,
        );

        match result {
            Ok(_) => panic!("expected --extended-hours validation failure"),
            Err(error) => {
                assert_eq!(error.to_string(), "--extended-hours requires --limit-price");
            }
        }
    }

    #[tokio::test]
    async fn cli_env_loads_dry_run_config() {
        let config_dir = tempfile::tempdir().unwrap();
        let config_path = config_dir.path().join("config.toml");
        let secrets_path = config_dir.path().join("secrets.toml");

        std::fs::write(
            &config_path,
            r#"
                database_url = ":memory:"
                server_port = 8080
                board_port = 8081
                apalis_finished_job_cleanup_interval_secs = 3600

                [assets.equities]

                [raindex]
                orderbook = "0x1111111111111111111111111111111111111111"
                deployment_block = 1
                required_confirmations = 3
                ingestion_cutoff = "safe"

                [wallet]
                kind = "private-key"
                address = "0x0000000000000000000000000000000000000001"
            "#,
        )
        .unwrap();

        std::fs::write(
            &secrets_path,
            r#"
                [evm]
                rpc_url = "http://localhost:8545"
                base_rpc_url = "https://base.example.com"
                ethereum_rpc_url = "https://mainnet.infura.io"

                [broker]
                type = "dry-run"

                [wallet]
                private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

                [issuance]
                base_url = "http://issuance.test:8000"
                api_key = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
            "#,
        )
        .unwrap();

        let (ctx, command) = CliEnv::try_parse_from([
            "st0x-cli",
            "--config",
            config_path.to_str().unwrap(),
            "--secrets",
            secrets_path.to_str().unwrap(),
            "buy",
            "-s",
            "AAPL",
            "-q",
            "1",
        ])
        .unwrap()
        .load()
        .await
        .unwrap();

        assert!(matches!(command, Commands::Buy { .. }));
        assert_eq!(ctx.database_url, ":memory:");
        assert_eq!(ctx.evm.required_confirmations, 3);
        assert_eq!(ctx.evm.ingestion_cutoff, IngestionCutoff::Safe);
        assert!(matches!(ctx.broker, BrokerCtx::DryRun));
    }
}
