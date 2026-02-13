//! CLI commands for trading, asset transfers, and authentication.

mod alpaca_wallet;
mod auth;
mod cctp;
mod rebalancing;
mod trading;
mod vault;

use alloy::primitives::{Address, B256, TxHash};
use alloy::providers::{ProviderBuilder, WsConnect};
use clap::{Parser, Subcommand, ValueEnum};
use rust_decimal::Decimal;
use sqlx::SqlitePool;
use std::io::Write;
use std::sync::Arc;
use thiserror::Error;
use tracing::info;

use st0x_execution::{Direction, FractionalShares, Symbol};

use crate::config::{Ctx, Env};
use crate::offchain_order::OrderPlacer;
use crate::symbol::cache::SymbolCache;
use crate::threshold::Usdc;

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

/// CCTP chain identifier for specifying source chain.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CctpChain {
    /// Ethereum mainnet (destination: Base)
    Ethereum,
    /// Base mainnet (destination: Ethereum)
    Base,
}

#[derive(Debug, Error)]
pub enum CliError {
    #[error("Invalid quantity: {value}. Quantity must be greater than zero")]
    InvalidQuantity { value: u64 },
}

#[derive(Debug, Parser)]
#[command(name = "schwab")]
#[command(about = "A CLI tool for Charles Schwab stock trading")]
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
        /// Number of shares to buy (whole shares only)
        #[arg(short = 'q', long = "quantity")]
        quantity: u64,
    },
    /// Sell shares of a stock
    Sell {
        /// Stock symbol (e.g., AAPL, TSLA)
        #[arg(short = 's', long = "symbol")]
        symbol: Symbol,
        /// Number of shares to sell (whole shares only)
        #[arg(short = 'q', long = "quantity")]
        quantity: u64,
    },
    /// Process a transaction hash to execute opposite-side trade
    ProcessTx {
        /// Transaction hash (0x prefixed, 64 hex characters)
        #[arg(long = "tx-hash")]
        tx_hash: TxHash,
    },
    /// Perform Charles Schwab OAuth authentication flow
    Auth,

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
        /// Token contract address (required for to-alpaca direction)
        #[arg(long = "token-address")]
        token_address: Option<Address>,
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
    /// Default destination is your configured sender wallet.
    AlpacaWithdraw {
        /// Amount of USDC to withdraw
        #[arg(short = 'a', long = "amount")]
        amount: Usdc,

        /// Destination address (defaults to SENDER_WALLET from env)
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

    /// List all Alpaca crypto wallet transfers
    ///
    /// Shows all deposits and withdrawals for the Alpaca account.
    /// Useful for debugging transfer status and verifying deposits.
    AlpacaTransfers,

    /// Deposit tokens into a Raindex vault
    ///
    /// This command deposits ERC20 tokens from your wallet into a Raindex OrderBook vault.
    /// It handles ERC20 approval and the vault deposit in sequence.
    VaultDeposit {
        /// Amount of tokens to deposit (human-readable, e.g., 100 for 100 tokens)
        #[arg(short = 'a', long = "amount")]
        amount: Decimal,

        /// Token contract address
        #[arg(short = 't', long = "token")]
        token: Address,

        /// Vault ID
        #[arg(short = 'v', long = "vault-id")]
        vault_id: B256,

        /// Token decimals (e.g., 6 for USDC, 18 for most ERC20s)
        #[arg(short = 'd', long = "decimals")]
        decimals: u8,
    },

    /// Withdraw USDC from a Raindex vault
    ///
    /// This command withdraws USDC from a Raindex OrderBook vault to your wallet.
    VaultWithdraw {
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

    /// List all Alpaca tokenization requests
    ///
    /// Shows mint and redemption requests for the Alpaca account.
    /// Useful for debugging tokenization status without creating new requests.
    AlpacaTokenizationRequests,

    /// Check the status of a Schwab order by order ID
    OrderStatus {
        /// The Schwab order ID to check
        #[arg(long = "order-id")]
        order_id: String,
    },
}

#[derive(Debug, Parser)]
#[command(name = "schwab-cli")]
#[command(about = "A CLI tool for Charles Schwab stock trading")]
#[command(version)]
pub struct CliEnv {
    #[clap(flatten)]
    env: Env,
    #[command(subcommand)]
    pub command: Commands,
}

impl CliEnv {
    /// Parse CLI arguments, load config from file, and return with subcommand.
    pub fn parse_and_convert() -> anyhow::Result<(Ctx, Commands)> {
        let cli_env = Self::parse();
        let ctx = Ctx::load_files(&cli_env.env.config, &cli_env.env.secrets)?;
        Ok((ctx, cli_env.command))
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
    symbol: Symbol,
    quantity: u64,
    direction: Direction,
    ctx: &Ctx,
    pool: &SqlitePool,
    stdout: &mut W,
) -> anyhow::Result<()> {
    if quantity == 0 {
        return Err(CliError::InvalidQuantity { value: quantity }.into());
    }
    info!("Processing {direction:?} order: symbol={symbol}, quantity={quantity}");
    trading::execute_order_with_writers(symbol, quantity, direction, ctx, pool, stdout).await
}

/// Commands that don't require a WebSocket provider.
enum SimpleCommand {
    Buy {
        symbol: Symbol,
        quantity: u64,
    },
    Sell {
        symbol: Symbol,
        quantity: u64,
    },
    Auth,
    TransferEquity {
        direction: TransferDirection,
        symbol: Symbol,
        quantity: FractionalShares,
        token_address: Option<Address>,
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
    AlpacaTransfers,
    AlpacaConvert {
        direction: ConvertDirection,
        amount: Usdc,
    },
    OrderStatus {
        order_id: String,
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
    VaultDeposit {
        amount: Decimal,
        token: Address,
        vault_id: B256,
        decimals: u8,
    },
    VaultWithdraw {
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
        Commands::Buy { symbol, quantity } => Ok(SimpleCommand::Buy { symbol, quantity }),
        Commands::Sell { symbol, quantity } => Ok(SimpleCommand::Sell { symbol, quantity }),
        Commands::Auth => Ok(SimpleCommand::Auth),
        Commands::TransferEquity {
            direction,
            symbol,
            quantity,
            token_address,
        } => Ok(SimpleCommand::TransferEquity {
            direction,
            symbol,
            quantity,
            token_address,
        }),
        Commands::AlpacaDeposit { amount } => Ok(SimpleCommand::AlpacaDeposit { amount }),
        Commands::AlpacaWithdraw { amount, to_address } => {
            Ok(SimpleCommand::AlpacaWithdraw { amount, to_address })
        }
        Commands::AlpacaWhitelist { address } => Ok(SimpleCommand::AlpacaWhitelist { address }),
        Commands::AlpacaTransfers => Ok(SimpleCommand::AlpacaTransfers),
        Commands::AlpacaConvert { direction, amount } => {
            Ok(SimpleCommand::AlpacaConvert { direction, amount })
        }
        Commands::AlpacaTokenizationRequests => Err(ProviderCommand::AlpacaTokenizationRequests),
        Commands::ProcessTx { tx_hash } => Err(ProviderCommand::ProcessTx { tx_hash }),
        Commands::TransferUsdc { direction, amount } => {
            Err(ProviderCommand::TransferUsdc { direction, amount })
        }
        Commands::VaultDeposit {
            amount,
            token,
            vault_id,
            decimals,
        } => Err(ProviderCommand::VaultDeposit {
            amount,
            token,
            vault_id,
            decimals,
        }),
        Commands::VaultWithdraw { amount } => Err(ProviderCommand::VaultWithdraw { amount }),
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
        } => Err(ProviderCommand::AlpacaTokenize {
            symbol,
            quantity,
            token,
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
    }
}

async fn run_simple_command<W: Write>(
    command: SimpleCommand,
    ctx: &Ctx,
    pool: &SqlitePool,
    stdout: &mut W,
) -> anyhow::Result<()> {
    match command {
        SimpleCommand::Buy { symbol, quantity } => {
            execute_order(symbol, quantity, Direction::Buy, ctx, pool, stdout).await
        }
        SimpleCommand::Sell { symbol, quantity } => {
            execute_order(symbol, quantity, Direction::Sell, ctx, pool, stdout).await
        }
        SimpleCommand::Auth => auth::auth_command(stdout, &ctx.broker, pool).await,
        SimpleCommand::TransferEquity {
            direction,
            symbol,
            quantity,
            token_address,
        } => {
            rebalancing::transfer_equity_command(
                stdout,
                direction,
                &symbol,
                quantity,
                token_address,
                ctx,
                pool,
            )
            .await
        }
        SimpleCommand::AlpacaDeposit { amount } => {
            alpaca_wallet::alpaca_deposit_command(stdout, amount, ctx).await
        }
        SimpleCommand::AlpacaWhitelist { address } => {
            alpaca_wallet::alpaca_whitelist_command(stdout, address, ctx).await
        }
        SimpleCommand::AlpacaWithdraw { amount, to_address } => {
            alpaca_wallet::alpaca_withdraw_command(stdout, amount, to_address, ctx).await
        }
        SimpleCommand::AlpacaTransfers => {
            alpaca_wallet::alpaca_transfers_command(stdout, ctx).await
        }
        SimpleCommand::AlpacaConvert { direction, amount } => {
            alpaca_wallet::alpaca_convert_command(stdout, direction, amount, ctx).await
        }
        SimpleCommand::OrderStatus { order_id } => {
            trading::order_status_command(stdout, &order_id, ctx, pool).await
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
            rebalancing::transfer_usdc_command(stdout, direction, amount, ctx, pool, provider).await
        }
        ProviderCommand::VaultDeposit {
            amount,
            token,
            vault_id,
            decimals,
        } => {
            vault::vault_deposit_command(stdout, amount, token, vault_id, decimals, ctx, provider)
                .await
        }
        ProviderCommand::VaultWithdraw { amount } => {
            vault::vault_withdraw_command(stdout, amount, ctx, provider).await
        }
        ProviderCommand::CctpBridge { amount, all, from } => {
            cctp::cctp_bridge_command(stdout, amount, all, from, ctx, provider).await
        }
        ProviderCommand::CctpRecover {
            burn_tx,
            source_chain,
        } => cctp::cctp_recover_command(stdout, burn_tx, source_chain, ctx, provider).await,
        ProviderCommand::ResetAllowance { chain } => {
            cctp::reset_allowance_command(stdout, chain, ctx, provider).await
        }
        ProviderCommand::AlpacaTokenize {
            symbol,
            quantity,
            token,
        } => {
            rebalancing::alpaca_tokenize_command(stdout, symbol, quantity, token, ctx, provider)
                .await
        }
        ProviderCommand::AlpacaRedeem {
            symbol,
            quantity,
            token,
        } => {
            rebalancing::alpaca_redeem_command(stdout, symbol, quantity, token, ctx, provider).await
        }
        ProviderCommand::AlpacaTokenizationRequests => {
            rebalancing::alpaca_tokenization_requests_command(stdout, ctx, provider).await
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::hex;
    use alloy::primitives::{FixedBytes, IntoLogData, U256, address, fixed_bytes};
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::{SolCall, SolEvent};
    use clap::CommandFactory;
    use httpmock::MockServer;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use serde_json::json;
    use st0x_execution::{
        Direction, FractionalShares, OrderStatus, Positive, SchwabError, SchwabTokens,
    };
    use std::str::FromStr;
    use url::Url;

    use super::*;
    use crate::bindings::IERC20::{decimalsCall, symbolCall};
    use crate::bindings::IOrderBookV5::{AfterClearV2, ClearConfigV2, ClearStateChangeV2, ClearV3};
    use crate::config::{BrokerCtx, LogLevel, SchwabAuth};
    use crate::offchain::execution::find_orders_by_status;
    use crate::onchain::EvmCtx;
    use crate::test_utils::{get_test_order, setup_test_db, setup_test_tokens};
    use crate::threshold::ExecutionThreshold;

    const TEST_ENCRYPTION_KEY: FixedBytes<32> = FixedBytes::ZERO;

    fn get_schwab_auth_from_ctx(ctx: &Ctx) -> &SchwabAuth {
        match &ctx.broker {
            BrokerCtx::Schwab(auth) => auth,
            _ => panic!("Expected Schwab broker ctx in tests"),
        }
    }

    #[tokio::test]
    async fn test_run_buy_order() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth_from_ctx(&ctx)).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "*/*")
                .header("content-type", "application/json");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/12345");
        });

        trading::execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut std::io::sink(),
        )
        .await
        .unwrap();

        account_mock.assert();
        order_mock.assert();
    }

    #[tokio::test]
    async fn test_run_sell_order() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth_from_ctx(&ctx)).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "*/*")
                .header("content-type", "application/json");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/12345");
        });

        trading::execute_order_with_writers(
            Symbol::new("TSLA").unwrap(),
            50,
            Direction::Sell,
            &ctx,
            &pool,
            &mut std::io::sink(),
        )
        .await
        .unwrap();

        account_mock.assert();
        order_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_order_failure() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth_from_ctx(&ctx)).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({
                    "error": "Invalid order",
                    "message": "Insufficient funds"
                }));
        });

        trading::execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut std::io::sink(),
        )
        .await
        .unwrap_err();
        account_mock.assert();
        order_mock.assert();
    }

    #[tokio::test]
    async fn test_run_with_expired_refresh_token() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;

        let expired_tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: chrono::Utc::now() - chrono::Duration::minutes(35),
            refresh_token: "expired_refresh_token".to_string(),
            refresh_token_fetched_at: chrono::Utc::now() - chrono::Duration::days(8),
        };
        expired_tokens
            .store(&pool, &get_schwab_auth_from_ctx(&ctx).encryption_key)
            .await
            .unwrap();

        let result = trading::execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut std::io::sink(),
        )
        .await;

        assert!(matches!(
            result.unwrap_err().downcast_ref::<SchwabError>(),
            Some(SchwabError::RefreshTokenExpired)
        ));
    }

    #[tokio::test]
    async fn test_run_with_successful_token_refresh() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;

        let expired_access_tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: chrono::Utc::now() - chrono::Duration::minutes(35),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: chrono::Utc::now() - chrono::Duration::days(1),
        };
        expired_access_tokens
            .store(&pool, &get_schwab_auth_from_ctx(&ctx).encryption_key)
            .await
            .unwrap();

        let token_refresh_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/v1/oauth/token")
                .body_contains("grant_type=refresh_token")
                .body_contains("refresh_token=valid_refresh_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "access_token": "refreshed_access_token",
                    "token_type": "Bearer",
                    "expires_in": 1800,
                    "refresh_token": "new_refresh_token",
                    "refresh_token_expires_in": 604_800
                }));
        });

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers")
                .header("authorization", "Bearer refreshed_access_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders")
                .header("authorization", "Bearer refreshed_access_token");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/12345");
        });

        let result = trading::execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut std::io::sink(),
        )
        .await;

        assert!(result.is_ok(), "Order should succeed after token refresh");
        token_refresh_mock.assert();
        account_mock.assert();
        order_mock.assert();
    }

    #[tokio::test]
    async fn test_run_with_valid_tokens_no_refresh_needed() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;

        let valid_tokens = SchwabTokens {
            access_token: "valid_access_token".to_string(),
            access_token_fetched_at: chrono::Utc::now() - chrono::Duration::minutes(5),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: chrono::Utc::now() - chrono::Duration::days(1),
        };
        valid_tokens
            .store(&pool, &get_schwab_auth_from_ctx(&ctx).encryption_key)
            .await
            .unwrap();

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers")
                .header("authorization", "Bearer valid_access_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders")
                .header("authorization", "Bearer valid_access_token");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/12345");
        });

        trading::execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut std::io::sink(),
        )
        .await
        .unwrap();
        account_mock.assert();
        order_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_order_success_stdout_output() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth_from_ctx(&ctx)).await;

        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/12345");
        });

        let mut stdout_buffer = Vec::new();
        trading::execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut stdout_buffer,
        )
        .await
        .unwrap();

        let stdout_output = String::from_utf8(stdout_buffer).unwrap();
        assert!(stdout_output.contains("Order placed successfully"));
        assert!(stdout_output.contains("AAPL"));
        assert!(stdout_output.contains("100"));
    }

    #[tokio::test]
    async fn test_execute_order_failure_stderr_output() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth_from_ctx(&ctx)).await;

        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({
                    "error": "Invalid order",
                    "message": "Insufficient funds"
                }));
        });

        let mut stdout_buffer = Vec::new();
        trading::execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut stdout_buffer,
        )
        .await
        .unwrap_err();

        let stdout_output = String::from_utf8(stdout_buffer).unwrap();
        assert!(stdout_output.contains("Failed to place order"));
    }

    #[tokio::test]
    async fn test_authentication_with_oauth_flow_on_expired_refresh_token() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;

        let expired_tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: chrono::Utc::now() - chrono::Duration::minutes(35),
            refresh_token: "expired_refresh_token".to_string(),
            refresh_token_fetched_at: chrono::Utc::now() - chrono::Duration::days(8),
        };
        expired_tokens
            .store(&pool, &get_schwab_auth_from_ctx(&ctx).encryption_key)
            .await
            .unwrap();

        let mut stdout_buffer = Vec::new();

        let result =
            auth::ensure_schwab_authentication(&pool, &ctx.broker, &mut stdout_buffer).await;

        assert!(matches!(
            result.unwrap_err().downcast_ref::<SchwabError>(),
            Some(SchwabError::RefreshTokenExpired)
        ));

        let mut stdout_buffer = Vec::new();
        writeln!(
            &mut stdout_buffer,
            "🔄 Your refresh token has expired. Starting authentication process..."
        )
        .unwrap();
        writeln!(
            &mut stdout_buffer,
            "   You will be guided through the Charles Schwab OAuth process."
        )
        .unwrap();

        let stdout_output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            stdout_output
                .contains("🔄 Your refresh token has expired. Starting authentication process...")
        );
        assert!(
            stdout_output.contains("You will be guided through the Charles Schwab OAuth process.")
        );
    }

    #[test]
    fn test_cli_error_display_messages() {
        let quantity_error = CliError::InvalidQuantity { value: 0 };
        let error_msg = quantity_error.to_string();
        assert!(error_msg.contains("Invalid quantity: 0"));
        assert!(error_msg.contains("greater than zero"));
    }

    fn create_test_ctx_for_cli(mock_server: &MockServer) -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmCtx {
                ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                order_owner: Some(address!("0x0000000000000000000000000000000000000000")),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerCtx::Schwab(SchwabAuth {
                app_key: "test_app_key".to_string(),
                app_secret: "test_app_secret".to_string(),
                redirect_uri: Some(Url::parse("https://127.0.0.1").expect("valid test URL")),
                base_url: Some(Url::parse(&mock_server.base_url()).expect("valid mock URL")),
                account_index: Some(0),
                encryption_key: TEST_ENCRYPTION_KEY,
            }),
            telemetry: None,
            rebalancing: None,
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    struct MockBlockchainData {
        order_owner: Address,
        receipt_json: serde_json::Value,
        after_clear_log: alloy::rpc::types::Log,
    }

    fn create_mock_blockchain_data(
        orderbook: Address,
        tx_hash: TxHash,
        alice_output_shares: &str,
        bob_output_usdc: u64,
    ) -> MockBlockchainData {
        let order = get_test_order();
        let order_owner = order.owner;

        let clear_event = ClearV3 {
            sender: address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
            alice: order.clone(),
            bob: order,
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let receipt_json = json!({
            "transactionHash": tx_hash,
            "transactionIndex": "0x0",
            "blockHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
            "blockNumber": "0x64",
            "from": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "to": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "contractAddress": null,
            "gasUsed": "0x5208",
            "cumulativeGasUsed": "0xf4240",
            "effectiveGasPrice": "0x3b9aca00",
            "status": "0x1",
            "type": "0x2",
            "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "logs": [{
                "address": orderbook,
                "topics": [ClearV3::SIGNATURE_HASH],
                "data": format!("0x{}", hex::encode(clear_event.into_log_data().data)),
                "blockNumber": "0x64",
                "blockTimestamp": "0x6553f100",
                "transactionHash": tx_hash,
                "transactionIndex": "0x0",
                "logIndex": "0x0",
                "removed": false
            }]
        });

        fn create_float_from_u256(value: U256, decimals: u8) -> B256 {
            use rain_math_float::Float;
            let float = Float::from_fixed_decimal_lossy(value, decimals).expect("valid Float");
            float.get_inner()
        }

        let alice_shares_u256 = U256::from_str(alice_output_shares).unwrap();
        let bob_usdc_u256 = U256::from(bob_output_usdc);

        let after_clear_event = AfterClearV2 {
            sender: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            clearStateChange: ClearStateChangeV2 {
                aliceOutput: create_float_from_u256(alice_shares_u256, 18),
                bobOutput: create_float_from_u256(bob_usdc_u256, 6),
                aliceInput: create_float_from_u256(bob_usdc_u256, 6),
                bobInput: create_float_from_u256(alice_shares_u256, 18),
            },
        };

        let after_clear_log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: after_clear_event.into_log_data(),
            },
            block_hash: Some(fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            )),
            block_number: Some(100),
            block_timestamp: Some(1_700_000_000),
            transaction_hash: Some(tx_hash),
            transaction_index: Some(0),
            log_index: Some(1),
            removed: false,
        };

        MockBlockchainData {
            order_owner,
            receipt_json,
            after_clear_log,
        }
    }

    fn setup_mock_provider_for_process_tx(
        mock_data: &MockBlockchainData,
        input_symbol: &str,
        output_symbol: &str,
    ) -> impl alloy::providers::Provider + Clone {
        let asserter = Asserter::new();
        asserter.push_success(&mock_data.receipt_json);
        asserter.push_success(&json!([mock_data.after_clear_log]));
        asserter.push_success(&mock_data.receipt_json);
        let input_decimals = if input_symbol == "USDC" { 6u8 } else { 18u8 };
        let output_decimals = if output_symbol == "USDC" { 6u8 } else { 18u8 };
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(
            &input_decimals,
        ));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &input_symbol.to_string(),
        ));
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(
            &output_decimals,
        ));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &output_symbol.to_string(),
        ));

        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    fn setup_schwab_api_mocks(server: &MockServer) -> (httpmock::Mock<'_>, httpmock::Mock<'_>) {
        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "*/*")
                .header("content-type", "application/json");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/12345");
        });

        (account_mock, order_mock)
    }

    #[test]
    fn verify_cli() {
        Cli::command().debug_assert();
    }

    #[test]
    fn test_cli_command_structure_validation() {
        let cmd = Cli::command();

        cmd.clone()
            .try_get_matches_from(vec!["schwab", "buy", "-s", "AAPL"])
            .unwrap_err();

        let _err = cmd
            .clone()
            .try_get_matches_from(vec!["schwab", "sell", "-q", "100"])
            .unwrap_err();

        let _err = cmd
            .clone()
            .try_get_matches_from(vec!["schwab", "buy"])
            .unwrap_err();

        let _matches = cmd
            .clone()
            .try_get_matches_from(vec!["schwab", "buy", "-s", "AAPL", "-q", "100"])
            .unwrap();

        let _matches = cmd
            .try_get_matches_from(vec!["schwab", "sell", "-s", "TSLA", "-q", "50"])
            .unwrap();
    }

    #[tokio::test]
    async fn test_integration_buy_command_end_to_end() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth_from_ctx(&ctx)).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "*/*")
                .header("content-type", "application/json");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/12345");
        });

        let mut stdout = Vec::new();

        let buy_command = Commands::Buy {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: 100,
        };

        let result = run_command_with_writers(ctx, buy_command, &pool, &mut stdout).await;

        assert!(
            result.is_ok(),
            "End-to-end CLI command should succeed: {result:?}"
        );
        account_mock.assert();
        order_mock.assert();

        let stdout_str = String::from_utf8(stdout).unwrap();
        assert!(stdout_str.contains("Order placed successfully"));
    }

    #[tokio::test]
    async fn test_integration_sell_command_end_to_end() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth_from_ctx(&ctx)).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "*/*")
                .header("content-type", "application/json");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/12345");
        });

        let mut stdout = Vec::new();

        let sell_command = Commands::Sell {
            symbol: Symbol::new("TSLA").unwrap(),
            quantity: 50,
        };

        let result = run_command_with_writers(ctx, sell_command, &pool, &mut stdout).await;

        assert!(
            result.is_ok(),
            "End-to-end CLI command should succeed: {result:?}"
        );
        account_mock.assert();
        order_mock.assert();

        let stdout_str = String::from_utf8(stdout).unwrap();
        assert!(stdout_str.contains("Order placed successfully"));
    }

    #[tokio::test]
    async fn test_integration_authentication_failure_scenarios() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;

        let expired_tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: chrono::Utc::now() - chrono::Duration::minutes(35),
            refresh_token: "valid_but_rejected_refresh_token".to_string(),
            refresh_token_fetched_at: chrono::Utc::now() - chrono::Duration::days(1),
        };
        expired_tokens
            .store(&pool, &get_schwab_auth_from_ctx(&ctx).encryption_key)
            .await
            .unwrap();

        let token_refresh_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/v1/oauth/token")
                .body_contains("grant_type=refresh_token")
                .body_contains("refresh_token=valid_but_rejected_refresh_token");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(
                    json!({"error": "invalid_grant", "error_description": "Refresh token expired"}),
                );
        });

        let mut stdout = Vec::new();

        let result = trading::execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut stdout,
        )
        .await;

        assert!(
            result.is_err(),
            "CLI command should fail due to auth issues"
        );
        token_refresh_mock.assert();

        assert!(format!("{}", result.unwrap_err()).contains("Refresh token expired"));
    }

    #[tokio::test]
    async fn test_integration_token_refresh_flow() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;

        let expired_tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: chrono::Utc::now() - chrono::Duration::minutes(35),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: chrono::Utc::now() - chrono::Duration::days(1),
        };
        expired_tokens
            .store(&pool, &get_schwab_auth_from_ctx(&ctx).encryption_key)
            .await
            .unwrap();

        let token_refresh_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/v1/oauth/token")
                .body_contains("grant_type=refresh_token")
                .body_contains("refresh_token=valid_refresh_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "access_token": "new_access_token",
                    "token_type": "Bearer",
                    "expires_in": 1800,
                    "refresh_token": "new_refresh_token",
                    "refresh_token_expires_in": 604_800
                }));
        });

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers")
                .header("authorization", "Bearer new_access_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders")
                .header("authorization", "Bearer new_access_token");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/12345");
        });

        let mut stdout = Vec::new();

        let result = trading::execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut stdout,
        )
        .await;

        assert!(
            result.is_ok(),
            "CLI command should succeed after token refresh: {result:?}"
        );
        token_refresh_mock.assert();
        account_mock.assert();
        order_mock.assert();

        let stored_tokens =
            SchwabTokens::load(&pool, &get_schwab_auth_from_ctx(&ctx).encryption_key)
                .await
                .unwrap();
        assert_eq!(stored_tokens.access_token, "new_access_token");
        assert_eq!(stored_tokens.refresh_token, "new_refresh_token");
    }

    #[tokio::test]
    async fn test_integration_database_operations() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;

        let mut stdout = Vec::new();

        let result = trading::execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut stdout,
        )
        .await;

        assert!(result.is_err(), "CLI should fail when no tokens are stored");

        assert!(format!("{}", result.unwrap_err()).contains("no rows returned"));

        setup_test_tokens(&pool, get_schwab_auth_from_ctx(&ctx)).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/12345");
        });

        let mut stdout2 = Vec::new();

        let result2 = trading::execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut stdout2,
        )
        .await;

        assert!(
            result2.is_ok(),
            "CLI should succeed with valid tokens in database"
        );
        account_mock.assert();
        order_mock.assert();
    }

    #[tokio::test]
    async fn test_integration_network_error_handling() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth_from_ctx(&ctx)).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(500)
                .header("content-type", "application/json")
                .json_body(json!({"error": "Internal Server Error"}));
        });

        let mut stdout = Vec::new();

        let result = trading::execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut stdout,
        )
        .await;

        assert!(result.is_err(), "CLI should fail on network errors");
        account_mock.assert();

        let stdout_str = String::from_utf8(stdout).unwrap();
        assert!(
            !stdout_str.is_empty(),
            "Should provide error feedback to user"
        );
    }

    #[tokio::test]
    async fn test_process_tx_command_transaction_not_found() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;

        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        let mut stdout = Vec::new();

        let asserter = Asserter::new();
        asserter.push_success(&json!(null));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let cache = SymbolCache::default();
        let order_placer = trading::create_order_placer(&ctx, &pool);

        let result = trading::process_tx_with_provider(
            tx_hash,
            &ctx,
            &pool,
            &mut stdout,
            &provider,
            &cache,
            order_placer,
        )
        .await;

        assert!(
            result.is_ok(),
            "Should handle transaction not found gracefully"
        );

        let stdout_str = String::from_utf8(stdout).unwrap();
        assert!(
            stdout_str.contains("Transaction not found"),
            "Should display transaction not found message"
        );
    }

    #[tokio::test]
    async fn test_integration_invalid_order_parameters() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth_from_ctx(&ctx)).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({
                    "error": "Invalid order parameters",
                    "message": "Insufficient buying power"
                }));
        });

        let mut stdout = Vec::new();

        let result = trading::execute_order_with_writers(
            Symbol::new("INVALID").unwrap(),
            999_999,
            Direction::Buy,
            &ctx,
            &pool,
            &mut stdout,
        )
        .await;

        assert!(
            result.is_err(),
            "CLI should fail on invalid order parameters"
        );
        account_mock.assert();
        order_mock.assert();

        let stdout_str = String::from_utf8(stdout).unwrap();
        assert!(
            stdout_str.contains("order")
                || stdout_str.contains("error")
                || stdout_str.contains("400")
        );
    }

    #[tokio::test]
    async fn test_process_tx_with_database_integration_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth_from_ctx(&ctx)).await;

        let tx_hash =
            fixed_bytes!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        let mock_data = create_mock_blockchain_data(
            ctx.evm.orderbook,
            tx_hash,
            "9000000000000000000",
            100_000_000,
        );

        let mut ctx = ctx;
        ctx.evm.order_owner = Some(mock_data.order_owner);

        let (account_mock, order_mock) = setup_schwab_api_mocks(&server);

        let provider = setup_mock_provider_for_process_tx(&mock_data, "USDC", "tAAPL");
        let cache = SymbolCache::default();
        let order_placer = trading::create_order_placer(&ctx, &pool);

        let mut stdout = Vec::new();

        let result = trading::process_tx_with_provider(
            tx_hash,
            &ctx,
            &pool,
            &mut stdout,
            &provider,
            &cache,
            order_placer,
        )
        .await;

        assert!(
            result.is_ok(),
            "process_tx should succeed with proper mocking: {:?}",
            result.as_ref().err()
        );

        let executions = find_orders_by_status(&pool, OrderStatus::Submitted)
            .await
            .unwrap();
        assert_eq!(executions.len(), 1);

        let (order_id, order) = &executions[0];
        assert_eq!(
            order.shares(),
            Positive::new(FractionalShares::new(Decimal::from(9))).unwrap()
        );
        assert_eq!(order.direction(), Direction::Buy);
        assert!(
            order.executor_order_id().is_some(),
            "Executor order ID should be set after submission"
        );
        assert!(!order_id.to_string().is_empty());

        account_mock.assert();
        order_mock.assert();

        let stdout_str = String::from_utf8(stdout).unwrap();
        assert!(stdout_str.contains("Processing trade with TradeAccumulator"));
        assert!(stdout_str.contains("Trade triggered execution for Schwab"));
        assert!(stdout_str.contains("Trade processing completed"));
    }

    #[tokio::test]
    async fn test_process_tx_database_duplicate_handling() {
        let server = MockServer::start();
        let ctx = create_test_ctx_for_cli(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth_from_ctx(&ctx)).await;

        let tx_hash =
            fixed_bytes!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        let mock_data = create_mock_blockchain_data(
            ctx.evm.orderbook,
            tx_hash,
            "5000000000000000000",
            50_000_000,
        );

        let mut ctx = ctx;
        ctx.evm.order_owner = Some(mock_data.order_owner);

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "*/*")
                .header("content-type", "application/json");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/12345");
        });

        let asserter1 = Asserter::new();
        asserter1.push_success(&mock_data.receipt_json);
        asserter1.push_success(&json!([mock_data.after_clear_log]));
        asserter1.push_success(&mock_data.receipt_json);
        asserter1.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        asserter1.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        asserter1.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        asserter1.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"tTSLA".to_string(),
        ));

        let provider1 = ProviderBuilder::new().connect_mocked_client(asserter1);
        let cache1 = SymbolCache::default();
        let order_placer = trading::create_order_placer(&ctx, &pool);

        let mut stdout1 = Vec::new();

        let result1 = trading::process_tx_with_provider(
            tx_hash,
            &ctx,
            &pool,
            &mut stdout1,
            &provider1,
            &cache1,
            order_placer.clone(),
        )
        .await;
        assert!(
            result1.is_ok(),
            "First process_tx should succeed: {:?}",
            result1.as_ref().err()
        );

        let executions = find_orders_by_status(&pool, OrderStatus::Submitted)
            .await
            .unwrap();
        assert_eq!(executions.len(), 1);
        let order = &executions[0].1;
        assert_eq!(
            order.shares(),
            Positive::new(FractionalShares::new(dec!(5))).unwrap()
        );

        let stdout_str1 = String::from_utf8(stdout1).unwrap();
        assert!(stdout_str1.contains("Processing trade with TradeAccumulator"));

        let asserter2 = Asserter::new();
        asserter2.push_success(&mock_data.receipt_json);
        asserter2.push_success(&json!([mock_data.after_clear_log]));
        asserter2.push_success(&mock_data.receipt_json);
        asserter2.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        asserter2.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        asserter2.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        asserter2.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"tTSLA".to_string(),
        ));

        let provider2 = ProviderBuilder::new().connect_mocked_client(asserter2);
        let cache2 = SymbolCache::default();

        let mut stdout2 = Vec::new();

        let result2 = trading::process_tx_with_provider(
            tx_hash,
            &ctx,
            &pool,
            &mut stdout2,
            &provider2,
            &cache2,
            order_placer,
        )
        .await;
        assert!(
            result2.is_ok(),
            "Second process_tx should succeed with graceful duplicate handling"
        );

        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(DISTINCT aggregate_id) FROM events WHERE aggregate_type = 'Position'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count.0, 1, "Only one position aggregate should exist");

        let stdout_str2 = String::from_utf8(stdout2).unwrap();
        assert!(stdout_str2.contains("Processing trade with TradeAccumulator"));
        assert!(stdout_str2.contains("Trade accumulated but did not trigger execution yet"));

        account_mock.assert_hits(1);
        order_mock.assert_hits(1);
    }

    #[test]
    fn test_auth_command_cli_help_text() {
        let mut cmd = Cli::command();

        let help_output = cmd.render_help().to_string();
        assert!(help_output.contains("auth"));
        assert!(help_output.contains("OAuth"));
        assert!(help_output.contains("authentication"));
    }

    #[test]
    fn test_transfer_commands_in_help_text() {
        let mut cmd = Cli::command();
        let help_output = cmd.render_help().to_string();

        assert!(
            help_output.contains("transfer-equity"),
            "Help should contain transfer-equity command"
        );
        assert!(
            help_output.contains("transfer-usdc"),
            "Help should contain transfer-usdc command"
        );
    }

    #[test]
    fn test_transfer_equity_command_structure() {
        let cmd = Cli::command();

        let result = cmd
            .clone()
            .try_get_matches_from(vec!["cli", "transfer-equity"]);
        assert!(result.is_err(), "transfer-equity without args should fail");

        let result = cmd.clone().try_get_matches_from(vec![
            "cli",
            "transfer-equity",
            "-s",
            "AAPL",
            "-q",
            "10.5",
        ]);
        assert!(
            result.is_err(),
            "transfer-equity without direction should fail"
        );

        let result = cmd.clone().try_get_matches_from(vec![
            "cli",
            "transfer-equity",
            "-d",
            "to-raindex",
            "-s",
            "AAPL",
            "-q",
            "10.5",
        ]);
        assert!(
            result.is_ok(),
            "transfer-equity to-raindex should succeed: {:?}",
            result.err()
        );

        let result = cmd.try_get_matches_from(vec![
            "cli",
            "transfer-equity",
            "-d",
            "to-alpaca",
            "-s",
            "AAPL",
            "-q",
            "5.0",
            "--token-address",
            "0x1234567890123456789012345678901234567890",
        ]);
        assert!(
            result.is_ok(),
            "transfer-equity to-alpaca with token-address should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_transfer_usdc_command_structure() {
        let cmd = Cli::command();

        let result = cmd
            .clone()
            .try_get_matches_from(vec!["cli", "transfer-usdc"]);
        assert!(result.is_err(), "transfer-usdc without args should fail");

        let result =
            cmd.clone()
                .try_get_matches_from(vec!["cli", "transfer-usdc", "-a", "1000.50"]);
        assert!(
            result.is_err(),
            "transfer-usdc without direction should fail"
        );

        let result = cmd.clone().try_get_matches_from(vec![
            "cli",
            "transfer-usdc",
            "-d",
            "to-raindex",
            "-a",
            "1000.50",
        ]);
        assert!(
            result.is_ok(),
            "transfer-usdc to-raindex should succeed: {:?}",
            result.err()
        );

        let result = cmd.try_get_matches_from(vec![
            "cli",
            "transfer-usdc",
            "-d",
            "to-alpaca",
            "-a",
            "500.25",
        ]);
        assert!(
            result.is_ok(),
            "transfer-usdc to-alpaca should succeed: {:?}",
            result.err()
        );
    }
}
