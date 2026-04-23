//! CLI commands for trading, asset transfers, and authentication.

mod alpaca_wallet;
mod cctp;
mod rebalancing;
mod submit;
mod trading;
mod vault;
mod wrapper;

use alloy::primitives::{Address, B256, TxHash};
use alloy::providers::{ProviderBuilder, WsConnect};
use clap::{Parser, Subcommand, ValueEnum};
use sqlx::SqlitePool;
use std::io::Write;
use std::sync::Arc;
use tracing::info;

use rain_math_float::Float;
use st0x_event_sorcery::Projection;
use st0x_evm::OpenChainErrorRegistry;
use st0x_execution::alpaca_broker_api::AlpacaLimitPrice;
use st0x_execution::{AlpacaAccountId, Direction, FractionalShares, Positive, Symbol, TimeInForce};
use st0x_finance::Usdc;

use crate::config::{Ctx, Env};
use crate::offchain_order::{OffchainOrder, OffchainOrderId, OrderPlacer};
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

fn parse_float(input: &str) -> Result<Float, String> {
    Float::parse(input.to_string()).map_err(|err| format!("{err}"))
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
    /// `assets.cash.vault_id` from config and forwarding into the generic
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
        /// Recipient wallet address (defaults to rebalancing signer address)
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

/// Commands that don't require a WebSocket provider.
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
    },
    WrapEquity {
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
    },
    UnwrapEquity {
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
}

/// Commands that require a WebSocket provider.
enum ProviderCommand {
    ProcessTx {
        tx_hash: TxHash,
    },
    TransferUsdc {
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
        } => Ok(SimpleCommand::TransferEquity {
            direction,
            symbol,
            quantity,
        }),
        Commands::WrapEquity { symbol, quantity } => {
            Ok(SimpleCommand::WrapEquity { symbol, quantity })
        }
        Commands::UnwrapEquity { symbol, quantity } => {
            Ok(SimpleCommand::UnwrapEquity { symbol, quantity })
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
        } => Err(ProviderCommand::AlpacaRedeem {
            symbol,
            quantity,
            token,
        }),
        Commands::OrderStatus { order_id } => Ok(SimpleCommand::OrderStatus { order_id }),
        Commands::Submit { to, data, yes } => Ok(SimpleCommand::Submit { to, data, yes }),
        Commands::RebuildView { aggregate, id, all } => {
            Ok(SimpleCommand::RebuildView { aggregate, id, all })
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
        } => {
            rebalancing::transfer_equity_command(stdout, direction, &symbol, quantity, ctx, pool)
                .await
        }
        SimpleCommand::WrapEquity { symbol, quantity } => {
            wrapper::wrap_equity_command(stdout, symbol, quantity, ctx).await
        }
        SimpleCommand::UnwrapEquity { symbol, quantity } => {
            wrapper::unwrap_equity_command(stdout, symbol, quantity, ctx).await
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
            vault::vault_deposit_command(stdout, deposit, ctx, pool).await
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
            vault::vault_withdraw_command(stdout, withdraw, ctx, pool).await
        }
        SimpleCommand::VaultWithdrawUsdc { amount } => {
            vault::vault_withdraw_usdc_command(stdout, amount, ctx, pool).await
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

async fn run_provider_command<W: Write>(
    command: ProviderCommand,
    ctx: &Ctx,
    pool: &SqlitePool,
    stdout: &mut W,
    order_placer: Arc<dyn OrderPlacer>,
) -> anyhow::Result<()> {
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(ctx.evm.ws_rpc_url.as_str()))
        .await?;

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
        } => rebalancing::alpaca_redeem_command(stdout, symbol, quantity, token, ctx).await,
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
    use crate::config::{AssetsConfig, BrokerCtx, EquitiesConfig, LogLevel, TradingMode};
    use crate::onchain::EvmCtx;
    use crate::test_utils::{positive_shares, setup_test_db};
    use crate::threshold::ExecutionThreshold;

    fn create_test_ctx() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmCtx {
                ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            trading_mode: TradingMode::Standalone {
                order_owner: Address::ZERO,
            },
            wallet: None,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
            travel_rule: None,
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
                apalis_finished_job_cleanup_interval_secs = 3600

                [assets.equities]

                [raindex]
                orderbook = "0x1111111111111111111111111111111111111111"
                deployment_block = 1
                order_owner = "0x2222222222222222222222222222222222222222"
            "#,
        )
        .unwrap();

        std::fs::write(
            &secrets_path,
            r#"
                [evm]
                ws_rpc_url = "ws://localhost:8545"

                [broker]
                type = "dry-run"
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
        assert!(matches!(ctx.broker, BrokerCtx::DryRun));
    }
}
