//! CLI commands for trading, asset transfers, and authentication.

mod alpaca_wallet;
mod cctp;
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

/// Transfer type for the fail-transfer command.
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

/// Aggregate types that have materialized views.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum AggregateView {
    /// Position aggregate (position_view)
    Position,
    /// Offchain order aggregate (offchain_order_view)
    OffchainOrder,
    /// Vault registry aggregate (vault_registry_view)
    VaultRegistry,
}

/// CCTP chain identifier for specifying source chain.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CctpChain {
    /// Ethereum mainnet (destination: Base)
    Ethereum,
    /// Base mainnet (destination: Ethereum)
    Base,
}

/// Manual repair operations for stuck local CQRS state.
#[derive(Debug, Subcommand)]
pub enum RepairCommand {
    /// Fail a position's pending offchain order pointer so normal hedging can retry.
    FailPendingOffchainOrder {
        /// Position symbol (e.g., MSTR)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Pending offchain order ID recorded on the position
        #[arg(short = 'o', long = "order-id")]
        order_id: OffchainOrderId,
        /// Reason to persist on the Position::FailOffChainOrder event
        #[arg(
            short = 'r',
            long = "reason",
            default_value = "Manually failed pending offchain order via CLI"
        )]
        reason: String,
    },
    /// Set a position's net exposure after an operator manual correction.
    #[command(group(
        ArgGroup::new("target")
            .required(true)
            .multiple(false)
            .args(["zero", "long", "short"])
    ))]
    SetPosition {
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
        #[arg(short = 'r', long = "reason")]
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
    /// Process a transaction hash to execute opposite-side trade
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

    /// Resume an interrupted USDC transfer by its id (Raindex <-> Alpaca)
    ///
    /// Re-drives a transfer whose CLI invocation was interrupted after the burn.
    /// The id is printed by `transfer-usdc` at start. An unknown id is rejected
    /// (never starts a fresh burn) and `--direction` must match the original;
    /// `--amount` is required for symmetry with the printed recovery command but
    /// is not validated -- the resume uses the aggregate's persisted amount.
    ResumeUsdcTransfer {
        /// Id of the transfer to resume (printed by `transfer-usdc`)
        #[arg(long = "id")]
        id: Uuid,
        /// Direction of the original transfer (must match the persisted transfer)
        #[arg(short = 'd', long = "direction")]
        direction: TransferDirection,
        /// Amount printed by `transfer-usdc`; required for symmetry but not
        /// validated -- the resume uses the persisted amount
        #[arg(short = 'a', long = "amount")]
        amount: Usdc,
    },

    /// Reconcile a USDC transfer stuck in the post-burn DepositFailed state
    ///
    /// Drives a post-burn `DepositFailed` USDC rebalance to the clearing
    /// terminal `Reconciled` state: the minted USDC was handled out-of-band, so
    /// this resolves the transfer (clearing the in-progress guard and
    /// reconciling source-venue inflight) rather than re-driving the deposit.
    /// Rejects an unknown id or an aggregate not in `DepositFailed`.
    ReconcileUsdcTransfer {
        /// Id of the stuck transfer to reconcile
        #[arg(long = "id")]
        id: Uuid,
        /// Why the transfer is being reconciled
        #[arg(short = 'r', long = "reason", default_value = "funds-moved-manually")]
        reason: ReconcileReasonArg,
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
    ///   `reconcile-usdc-transfer`. In-flight post-burn states (`Bridging`,
    ///   `AwaitingAttestation`, `Attested`, `Bridged`, `DepositInitiated`)
    ///   should be resumed with `resume-usdc-transfer`.
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

    /// Recover a stuck CCTP transfer by completing the mint on the destination chain.
    ///
    /// Use this when a CCTP burn succeeded but the mint wasn't completed (e.g., due to
    /// attestation polling being interrupted). Provide the burn transaction hash and
    /// specify the source chain to recover the transfer.
    CctpRecover {
        /// Transaction hash of the burn transaction on the source chain
        #[arg(long = "burn-tx")]
        burn_tx: TxHash,
        /// Source chain where the burn occurred
        #[arg(long = "source-chain")]
        source_chain: CctpChain,
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
        /// Stock symbol (e.g., AAPL, TSLA)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Number of shares to tokenize (supports fractional shares)
        #[arg(short = 'q', long = "quantity")]
        quantity: FractionalShares,
        /// Token contract address (to verify balance after tokenization)
        #[arg(short = 't', long = "token")]
        token: Address,
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
        /// Stock symbol (e.g., AAPL, TSLA)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Number of shares to redeem (supports fractional shares)
        #[arg(short = 'q', long = "quantity")]
        quantity: FractionalShares,
        /// Token contract address
        #[arg(short = 't', long = "token")]
        token: Address,
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

    /// Manually fail a stuck mint or redemption transfer
    ///
    /// Marks a transfer aggregate as failed, transitioning it to a terminal
    /// state. Use when a transfer is permanently stuck (e.g., timed out,
    /// unrecoverable error) and needs operator intervention.
    FailTransfer {
        /// Transfer type: "mint" or "redemption"
        #[arg(short = 't', long = "type")]
        transfer_type: TransferType,
        /// Aggregate ID (issuer_request_id for mint, redemption ID for redemption)
        #[arg(short = 'i', long = "id")]
        id: String,
        /// Reason for failure
        #[arg(
            short = 'r',
            long = "reason",
            default_value = "Manually failed via CLI"
        )]
        reason: String,
    },

    /// Re-check a failed mint or redemption and complete it if the provider settled it
    ///
    /// Delegates to the running bot's REST API so recovery dispatches through
    /// the in-process reactor (correcting live inventory). Requires the bot to
    /// be running and serving its API on the configured `server_port`.
    RecheckTransfer {
        /// Transfer type: "mint" or "redemption"
        #[arg(short = 't', long = "type")]
        transfer_type: TransferType,
        /// Aggregate ID (issuer_request_id for mint, redemption ID for redemption)
        #[arg(short = 'i', long = "id")]
        id: String,
    },

    /// Rebuild a materialized view by replaying all events from scratch
    ///
    /// Use as an escape hatch when a view becomes corrupted (e.g., due to
    /// lost updates from optimistic lock conflicts). Deletes the view row(s)
    /// and replays all events to reconstruct correct state.
    RebuildView {
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

    /// Repair stuck local CQRS state through aggregate commands.
    Repair {
        #[command(subcommand)]
        command: RepairCommand,
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
    Repair {
        command: RepairCommand,
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
    ReconcileUsdcTransfer {
        id: Uuid,
        reason: ReconcileReasonArg,
    },
    FailUsdcTransfer {
        id: Uuid,
        reason: String,
    },
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
        amount: Usdc,
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
        token: Address,
        recipient: Option<Address>,
    },
    AlpacaRedeem {
        symbol: Symbol,
        quantity: FractionalShares,
        token: Address,
        redemption_wallet: Option<Address>,
    },
    AlpacaTokenizationRequests,
}

async fn run_command_with_writers<W: Write>(
    ctx: Ctx,
    command: Commands,
    pool: &SqlitePool,
    stdout: &mut W,
) -> anyhow::Result<()> {
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
        Commands::ResumeUsdcTransfer {
            id,
            direction,
            amount,
        } => Err(ProviderCommand::ResumeUsdcTransfer {
            id,
            direction,
            amount,
        }),
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
        Commands::CctpRecover {
            burn_tx,
            source_chain,
        } => Err(ProviderCommand::CctpRecover {
            burn_tx,
            source_chain,
        }),
        Commands::ResetAllowance { chain } => Err(ProviderCommand::ResetAllowance { chain }),
        Commands::AlpacaTokenize {
            symbol,
            quantity,
            token,
            recipient,
        } => Err(ProviderCommand::AlpacaTokenize {
            symbol,
            quantity,
            token,
            recipient,
        }),
        Commands::AlpacaRedeem {
            symbol,
            quantity,
            token,
            redemption_wallet,
        } => Err(ProviderCommand::AlpacaRedeem {
            symbol,
            quantity,
            token,
            redemption_wallet,
        }),
        Commands::OrderStatus { order_id } => Ok(SimpleCommand::OrderStatus { order_id }),
        Commands::Submit { to, data, yes } => Ok(SimpleCommand::Submit { to, data, yes }),
        Commands::RebuildView { aggregate, id, all } => {
            Ok(SimpleCommand::RebuildView { aggregate, id, all })
        }
        Commands::Repair { command } => Ok(SimpleCommand::Repair { command }),
        Commands::FailTransfer {
            transfer_type,
            id,
            reason,
        } => Ok(SimpleCommand::FailTransfer {
            transfer_type,
            id,
            reason,
        }),
        Commands::RecheckTransfer { transfer_type, id } => {
            Ok(SimpleCommand::RecheckTransfer { transfer_type, id })
        }
        Commands::ReconcileUsdcTransfer { id, reason } => {
            Ok(SimpleCommand::ReconcileUsdcTransfer { id, reason })
        }
        Commands::FailUsdcTransfer { id, reason } => {
            Ok(SimpleCommand::FailUsdcTransfer { id, reason })
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
        SimpleCommand::Repair { command } => {
            run_repair_command(stdout, pool, command, ctx.execution_threshold).await
        }
        SimpleCommand::FailTransfer {
            transfer_type,
            id,
            reason,
        } => rebalancing::fail_transfer_command(stdout, pool, transfer_type, &id, &reason).await,
        SimpleCommand::RecheckTransfer { transfer_type, id } => {
            rebalancing::recheck_transfer_command(stdout, transfer_type, &id, ctx).await
        }
        SimpleCommand::ReconcileUsdcTransfer { id, reason } => {
            rebalancing::reconcile_usdc_transfer_command(stdout, id, reason.into(), pool).await
        }
        SimpleCommand::FailUsdcTransfer { id, reason } => {
            rebalancing::fail_usdc_transfer_command(stdout, id, &reason, pool).await
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
    }

    Ok(())
}

async fn run_repair_command<W: Write>(
    stdout: &mut W,
    pool: &SqlitePool,
    command: RepairCommand,
    execution_threshold: st0x_config::ExecutionThreshold,
) -> anyhow::Result<()> {
    match command {
        RepairCommand::FailPendingOffchainOrder {
            symbol,
            order_id,
            reason,
        } => {
            repair::fail_pending_offchain_order_command(stdout, pool, &symbol, order_id, reason)
                .await
        }
        RepairCommand::SetPosition {
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

/// Operator-chosen target for `repair set-position`, converted from the mutually
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
        ProviderCommand::ResumeUsdcTransfer {
            id,
            direction,
            amount,
        } => {
            rebalancing::resume_usdc_transfer_command(stdout, id, direction, amount, ctx, pool)
                .await
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
            token,
            recipient,
        } => {
            rebalancing::alpaca_tokenize_command(
                stdout, symbol, quantity, token, recipient, ctx, provider,
            )
            .await
        }
        ProviderCommand::AlpacaRedeem {
            symbol,
            quantity,
            token,
            redemption_wallet,
        } => {
            rebalancing::alpaca_redeem_command(
                stdout,
                symbol,
                quantity,
                token,
                redemption_wallet,
                ctx,
            )
            .await
        }
        ProviderCommand::AlpacaTokenizationRequests => {
            rebalancing::alpaca_tokenization_requests_command(stdout, ctx).await
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash, address};
    use clap::{CommandFactory, Parser};
    use url::Url;

    use super::*;
    use crate::test_utils::{positive_shares, setup_test_db};
    use st0x_config::EvmCtx;
    use st0x_config::ExecutionThreshold;
    use st0x_config::create_test_issuance_ctx;
    use st0x_config::{AssetsConfig, BrokerCtx, EquitiesConfig, LogLevel, TradingMode};

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
                | ProviderCommand::AlpacaTokenizationRequests,
            ) => panic!("expected process-tx provider command"),
            Ok(_) => panic!("expected provider command classification"),
        }
    }

    #[test]
    fn recheck_transfer_command_parses_type_and_id() {
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "recheck-transfer",
            "--type",
            "mint",
            "--id",
            "ISS001",
        ])
        .unwrap();

        match cli.command {
            Commands::RecheckTransfer { transfer_type, id } => {
                assert!(matches!(transfer_type, TransferType::Mint));
                assert_eq!(id, "ISS001");
            }
            other => panic!("expected recheck-transfer command, got: {other:?}"),
        }
    }

    #[test]
    fn resume_usdc_transfer_command_parses_id_direction_and_amount() {
        let id = Uuid::from_u128(42);
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "resume-usdc-transfer",
            "--id",
            &id.to_string(),
            "--direction",
            "to-raindex",
            "--amount",
            "100",
        ])
        .unwrap();

        match cli.command {
            Commands::ResumeUsdcTransfer {
                id: parsed_id,
                direction,
                amount,
            } => {
                assert_eq!(parsed_id, id);
                assert!(matches!(direction, TransferDirection::ToRaindex));
                assert_eq!(
                    amount,
                    Usdc::new(rain_math_float::Float::parse("100".to_string()).unwrap())
                );
            }
            other => panic!("expected resume-usdc-transfer command, got: {other:?}"),
        }
    }

    #[test]
    fn reconcile_usdc_transfer_command_parses_id_and_reason() {
        let id = Uuid::from_u128(77);
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "reconcile-usdc-transfer",
            "--id",
            &id.to_string(),
            "--reason",
            "deposit-credited-offline",
        ])
        .unwrap();

        match cli.command {
            Commands::ReconcileUsdcTransfer {
                id: parsed_id,
                reason,
            } => {
                assert_eq!(parsed_id, id);
                assert!(matches!(reason, ReconcileReasonArg::DepositCreditedOffline));
            }
            other => panic!("expected reconcile-usdc-transfer command, got: {other:?}"),
        }
    }

    #[test]
    fn reconcile_usdc_transfer_command_defaults_reason_to_funds_moved_manually() {
        let id = Uuid::from_u128(78);
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "reconcile-usdc-transfer",
            "--id",
            &id.to_string(),
        ])
        .unwrap();

        match cli.command {
            Commands::ReconcileUsdcTransfer { reason, .. } => {
                assert!(matches!(reason, ReconcileReasonArg::FundsMovedManually));
            }
            other => panic!("expected reconcile-usdc-transfer command, got: {other:?}"),
        }
    }

    #[test]
    fn classify_reconcile_usdc_transfer_command_as_simple() {
        // reconcile-usdc-transfer only loads + sends to the event store, so it
        // must route without an RPC provider.
        let command = Commands::ReconcileUsdcTransfer {
            id: Uuid::from_u128(42),
            reason: ReconcileReasonArg::FundsMovedManually,
        };

        match classify_command(command) {
            Ok(SimpleCommand::ReconcileUsdcTransfer { reason, .. }) => {
                assert!(matches!(reason, ReconcileReasonArg::FundsMovedManually));
            }
            Ok(_) => panic!("expected reconcile-usdc-transfer simple command"),
            Err(
                ProviderCommand::ProcessTx { .. }
                | ProviderCommand::TransferUsdc { .. }
                | ProviderCommand::ResumeUsdcTransfer { .. }
                | ProviderCommand::CctpBridge { .. }
                | ProviderCommand::CctpRecover { .. }
                | ProviderCommand::ResetAllowance { .. }
                | ProviderCommand::AlpacaTokenize { .. }
                | ProviderCommand::AlpacaRedeem { .. }
                | ProviderCommand::AlpacaTokenizationRequests,
            ) => panic!("expected simple command classification, got provider command"),
        }
    }

    #[test]
    fn classify_recheck_transfer_command_as_simple() {
        // The recheck-transfer command must route without an RPC provider:
        // it delegates to the running bot's REST API rather than touching chain.
        let command = Commands::RecheckTransfer {
            transfer_type: TransferType::Redemption,
            id: "redemption-1".to_string(),
        };

        match classify_command(command) {
            Ok(SimpleCommand::RecheckTransfer { transfer_type, id }) => {
                assert!(matches!(transfer_type, TransferType::Redemption));
                assert_eq!(id, "redemption-1");
            }
            Ok(_) => panic!("expected recheck-transfer simple command"),
            Err(
                ProviderCommand::ProcessTx { .. }
                | ProviderCommand::TransferUsdc { .. }
                | ProviderCommand::ResumeUsdcTransfer { .. }
                | ProviderCommand::CctpBridge { .. }
                | ProviderCommand::CctpRecover { .. }
                | ProviderCommand::ResetAllowance { .. }
                | ProviderCommand::AlpacaTokenize { .. }
                | ProviderCommand::AlpacaRedeem { .. }
                | ProviderCommand::AlpacaTokenizationRequests,
            ) => panic!("expected simple command classification, got provider command"),
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
    fn repair_fail_pending_offchain_order_parses() {
        let order_id = OffchainOrderId::new();
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "repair",
            "fail-pending-offchain-order",
            "--symbol",
            "MSTR",
            "--order-id",
            &order_id.to_string(),
            "--reason",
            "operator repair",
        ])
        .unwrap();

        match cli.command {
            Commands::Repair {
                command:
                    RepairCommand::FailPendingOffchainOrder {
                        symbol,
                        order_id: parsed_order_id,
                        reason,
                    },
            } => {
                assert_eq!(symbol, Symbol::new("MSTR").unwrap());
                assert_eq!(parsed_order_id, order_id);
                assert_eq!(reason, "operator repair");
            }
            other => panic!("expected repair command, got: {other:?}"),
        }
    }

    #[test]
    fn repair_set_position_zero_parses() {
        let cli = Cli::try_parse_from([
            "st0x-cli",
            "repair",
            "set-position",
            "--symbol",
            "SPYM",
            "--zero",
            "--reason",
            "manual rebalance completed",
        ])
        .unwrap();

        match cli.command {
            Commands::Repair {
                command:
                    RepairCommand::SetPosition {
                        symbol,
                        zero,
                        long,
                        short,
                        price,
                        reason,
                    },
            } => {
                assert_eq!(symbol, Symbol::new("SPYM").unwrap());
                assert!(zero);
                assert_eq!(long, None);
                assert_eq!(short, None);
                assert!(price.is_none());
                assert_eq!(reason, "manual rebalance completed");
            }
            other => panic!("expected repair set-position command, got: {other:?}"),
        }
    }

    #[test]
    fn repair_set_position_long_and_short_parse_positive_amounts() {
        let long = Cli::try_parse_from([
            "st0x-cli",
            "repair",
            "set-position",
            "--symbol",
            "SPYM",
            "--long",
            "100",
            "--reason",
            "manual buy",
        ])
        .unwrap();

        match long.command {
            Commands::Repair {
                command:
                    RepairCommand::SetPosition {
                        long: Some(quantity),
                        ..
                    },
            } => assert_eq!(quantity, positive_shares("100")),
            other => panic!("expected repair set-position --long, got: {other:?}"),
        }

        let short = Cli::try_parse_from([
            "st0x-cli",
            "repair",
            "set-position",
            "--symbol",
            "SPYM",
            "--short",
            "12.5",
            "--reason",
            "manual sell",
        ])
        .unwrap();

        match short.command {
            Commands::Repair {
                command:
                    RepairCommand::SetPosition {
                        short: Some(quantity),
                        ..
                    },
            } => assert_eq!(quantity, positive_shares("12.5")),
            other => panic!("expected repair set-position --short, got: {other:?}"),
        }
    }

    #[test]
    fn repair_set_position_rejects_missing_reason() {
        let error = Cli::try_parse_from([
            "st0x-cli",
            "repair",
            "set-position",
            "--symbol",
            "SPYM",
            "--zero",
        ])
        .unwrap_err();
        let rendered = error.to_string();
        assert!(
            rendered.contains("reason"),
            "unexpected clap error: {rendered}"
        );
    }

    #[test]
    fn repair_set_position_rejects_multiple_targets() {
        let error = Cli::try_parse_from([
            "st0x-cli",
            "repair",
            "set-position",
            "--symbol",
            "SPYM",
            "--zero",
            "--long",
            "1",
            "--reason",
            "operator repair",
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
    fn repair_set_position_rejects_zero_long_amount() {
        let error = Cli::try_parse_from([
            "st0x-cli",
            "repair",
            "set-position",
            "--symbol",
            "SPYM",
            "--long",
            "0",
            "--reason",
            "operator repair",
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
    fn classify_repair_command_as_simple() {
        let order_id = OffchainOrderId::new();
        let command = Commands::Repair {
            command: RepairCommand::FailPendingOffchainOrder {
                symbol: Symbol::new("MSTR").unwrap(),
                order_id,
                reason: "operator repair".to_string(),
            },
        };

        match classify_command(command) {
            Ok(SimpleCommand::Repair {
                command:
                    RepairCommand::FailPendingOffchainOrder {
                        symbol,
                        order_id: parsed_order_id,
                        reason,
                    },
            }) => {
                assert_eq!(symbol, Symbol::new("MSTR").unwrap());
                assert_eq!(parsed_order_id, order_id);
                assert_eq!(reason, "operator repair");
            }
            Ok(_) => panic!("expected repair simple command"),
            Err(
                ProviderCommand::ProcessTx { .. }
                | ProviderCommand::TransferUsdc { .. }
                | ProviderCommand::ResumeUsdcTransfer { .. }
                | ProviderCommand::CctpBridge { .. }
                | ProviderCommand::CctpRecover { .. }
                | ProviderCommand::ResetAllowance { .. }
                | ProviderCommand::AlpacaTokenize { .. }
                | ProviderCommand::AlpacaRedeem { .. }
                | ProviderCommand::AlpacaTokenizationRequests,
            ) => panic!("expected simple command classification"),
        }
    }

    #[tokio::test]
    async fn run_command_with_writers_executes_repair_set_position() {
        let ctx = create_test_ctx();
        let pool = setup_test_db().await;
        let symbol = Symbol::new("SPYM").unwrap();
        let command = Commands::Repair {
            command: RepairCommand::SetPosition {
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

    #[test]
    fn manual_position_target_converts_short_to_negative_net() {
        let target = ManualPositionTarget::from_flags(false, None, Some(positive_shares("12.5")))
            .unwrap()
            .net()
            .unwrap();
        let expected = (FractionalShares::ZERO - positive_shares("12.5").inner()).unwrap();

        assert_eq!(target, expected);
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
        assert!(matches!(ctx.broker, BrokerCtx::DryRun));
    }
}
