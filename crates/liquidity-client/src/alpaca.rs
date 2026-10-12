//! The `alpaca` command group of `st0x-liquidity-client`: T0 operator calls to
//! the account bound Alpaca gateway. The S01 operator client in st0x.issuance
//! (`crates/ops-client/src/alpaca.rs`) carries the same command set, and the
//! two must stay identical.

use std::error::Error as StdError;
use std::fmt::Display;
use std::io::Write;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand, ValueEnum};
use reqwest::StatusCode;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use st0x_alpaca::GatewayHopError;
use st0x_alpaca::broker::{
    ALPACA_MAX_DECIMAL_PLACES, AccountActivitiesQuery, AlpacaBrokerApiError as BrokerError,
    AlpacaLimitOrder, AlpacaLimitPrice, AssetDetails, ClientOrderId, ConversionOrder,
    ConversionOrders, Direction, MarketOrder, MarketSession,
};
use st0x_alpaca::core::Network as TokenizationNetwork;
use st0x_alpaca::st0x_finance::{
    FloatError, FractionalShares, NotPositive, Positive, Symbol, Usd, Usdc,
};
use st0x_alpaca::tokenization::{
    AlpacaTokenizationError as TokenizationError, IssuerRequestId, TokenizationLookups,
    TokenizationRequestId,
};
use st0x_alpaca::wallet::{
    AlpacaTransferId, AlpacaWalletError as WalletError, Network as WalletNetwork, TokenSymbol,
    WalletTransfers, WhitelistEntry,
};
use st0x_alpaca_gateway_api::client::{
    ClientError, GatewayBroker, GatewayClient, GatewayWallet, StaticToken,
};
use st0x_alpaca_gateway_api::dto::wallet::{
    TravelRulePatchRequest, WhitelistCreateRequest, WhitelistRemoveRequest, WhitelistResponse,
};
use st0x_alpaca_gateway_api::{ErrorBody, ErrorCode, Method, Operation, Outcome, Tier};
use url::Url;
use uuid::Uuid;

use crate::cli::nonblank_reason;

/// The gateway client's `DEADLINE_MARGIN`: how far past an operation's
/// deadline `RawGateway` waits, so the gateway's own deadline answer arrives
/// first.
const CLIENT_DEADLINE_MARGIN: Duration = Duration::from_secs(15);
/// The gateway client's `GATEWAY_CONNECT_TIMEOUT`. A connect it cuts never
/// left, so it reads as retryable rather than as an unknown outcome.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Decimal places Alpaca takes in a USD notional. `st0x-alpaca` floors a
/// finer `BuyWithUsd` amount to it (`truncate_notional_to_whole_cents`).
const USD_NOTIONAL_DECIMAL_PLACES: u8 = 2;

/// Operations available through an account bound Alpaca gateway: one flat
/// command list, typed along the gateway client's seams (`broker`, `wallet`,
/// and `tokenization`).
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    #[command(flatten)]
    Broker(BrokerCommand),
    #[command(flatten)]
    Wallet(WalletCommand),
    #[command(flatten)]
    Tokenization(TokenizationCommand),
}

/// The broker and market data operations.
#[derive(Debug, Subcommand)]
pub(crate) enum BrokerCommand {
    /// Account balance, buying power, and withdrawable cash.
    Account,
    /// Withdrawable cash in cents.
    Cash,
    /// All equity and cash positions.
    Positions,
    /// Current account position mark for one symbol.
    PositionMark {
        #[arg(long)]
        symbol: Symbol,
    },
    /// Account activities of one or more Alpaca activity types.
    Activities {
        #[arg(long = "type", required = true)]
        types: Vec<String>,
        #[arg(long)]
        after: Option<DateTime<Utc>>,
        #[arg(long)]
        until: Option<DateTime<Utc>>,
    },
    /// Current market session and its boundaries.
    MarketStatus,
    /// Latest trade price for one symbol.
    LatestTrade {
        #[arg(long)]
        symbol: Symbol,
    },
    /// Bid and ask for one symbol from the overnight feed or the delayed
    /// SIP feed.
    Quote {
        #[arg(long)]
        symbol: Symbol,
        #[arg(long, value_enum, default_value_t = QuoteFeed::Overnight)]
        feed: QuoteFeed,
    },
    /// Alpaca asset attributes for one symbol.
    Asset {
        #[arg(long)]
        symbol: Symbol,
    },
    /// Place a buy order.
    Buy(OrderArgs),
    /// Place a sell order.
    Sell(OrderArgs),
    /// Cancel an order by broker order ID.
    Cancel {
        order_id: Uuid,
        #[arg(long, value_parser = nonblank_reason)]
        reason: String,
    },
    /// Read an order by broker order ID.
    OrderStatus { order_id: Uuid },
    /// Find an operator order by its client order UUID.
    OrderFind { client_order_id: Uuid },
    /// Submit one USD and USDC conversion order and wait until it settles.
    Convert(ConvertArgs),
    /// Read a conversion by broker order ID.
    ConversionStatus { order_id: Uuid },
    /// Find a conversion by its client order UUID.
    ConversionFind { client_order_id: Uuid },
    /// Create a security journal to a configured counterparty.
    Journal {
        #[arg(long)]
        counterparty: String,
        #[arg(long)]
        symbol: Symbol,
        #[arg(long, value_parser = parse_positive_shares)]
        quantity: Positive<FractionalShares>,
        #[arg(long)]
        operation_id: Option<Uuid>,
        #[arg(long, value_parser = nonblank_reason)]
        reason: String,
    },
}

/// The crypto wallet operations.
#[derive(Debug, Subcommand)]
pub(crate) enum WalletCommand {
    /// Read the deposit address. This command does not submit a chain transfer.
    Deposit {
        #[arg(long, default_value = "USDC")]
        asset: String,
        #[arg(long)]
        network: String,
    },
    /// Initiate a withdrawal to an approved address.
    Withdraw {
        #[arg(long, value_parser = parse_positive_usdc)]
        amount: Positive<Usdc>,
        #[arg(long, default_value = "USDC")]
        asset: String,
        #[arg(long)]
        address: Address,
        #[arg(long)]
        operation_id: Option<Uuid>,
        #[arg(long, value_parser = nonblank_reason)]
        reason: String,
    },
    /// Add an address to the withdrawal whitelist.
    Whitelist {
        #[arg(long)]
        address: Address,
        #[arg(long, default_value = "USDC")]
        asset: String,
        #[arg(long)]
        operation_id: Option<Uuid>,
        #[arg(long, value_parser = nonblank_reason)]
        reason: String,
    },
    /// List withdrawal whitelist entries.
    WhitelistList,
    /// Apply the configured Travel Rule beneficiary to every whitelist entry.
    WhitelistPatchTravelRule {
        #[arg(long)]
        operation_id: Option<Uuid>,
        #[arg(long, value_parser = nonblank_reason)]
        reason: String,
    },
    /// Remove an address from the withdrawal whitelist.
    Unwhitelist {
        #[arg(long)]
        address: Address,
        #[arg(long)]
        operation_id: Option<Uuid>,
        #[arg(long, value_parser = nonblank_reason)]
        reason: String,
    },
    /// List wallet transfers, optionally retaining only pending rows.
    Transfers {
        #[arg(long)]
        pending: bool,
    },
    /// Read one wallet transfer and its reported fees.
    Transfer { transfer_id: Uuid },
    /// Find an incoming deposit by chain transaction hash.
    FindDeposit { tx_hash: B256 },
}

/// The tokenization operations, each bound to one network.
#[derive(Debug, Subcommand)]
pub(crate) enum TokenizationCommand {
    /// Request tokenization to an approved recipient.
    Tokenize {
        #[arg(long)]
        symbol: Symbol,
        #[arg(long, value_parser = parse_positive_shares)]
        quantity: Positive<FractionalShares>,
        #[arg(long)]
        recipient: Address,
        #[arg(long, value_enum)]
        network: Network,
        #[arg(long)]
        issuer_request_id: Option<Uuid>,
        #[arg(long, value_parser = nonblank_reason)]
        reason: String,
    },
    /// Read one tokenization request by provider request ID.
    TokenizationRequest {
        request_id: TokenizationRequestId,
        #[arg(long, value_enum)]
        network: Network,
    },
    /// Find a mint by caller supplied issuer request ID.
    MintStatus {
        issuer_request_id: IssuerRequestId,
        #[arg(long, value_enum)]
        network: Network,
    },
    /// Find the redemption created by a separate chain transfer.
    Redeem {
        tx_hash: B256,
        #[arg(long, value_enum)]
        network: Network,
    },
    /// List tokenization requests.
    TokenizationRequests {
        #[arg(long, value_enum)]
        network: Network,
        #[arg(long)]
        pending: bool,
    },
}

#[derive(Debug, Args)]
pub(crate) struct OrderArgs {
    #[arg(long)]
    symbol: Symbol,
    #[arg(long, value_parser = parse_order_quantity)]
    quantity: Positive<FractionalShares>,
    #[arg(long)]
    limit_price: Option<AlpacaLimitPrice>,
    #[arg(long, requires = "limit_price")]
    extended_hours: bool,
    #[arg(long)]
    client_order_id: Option<Uuid>,
    #[arg(long, value_parser = nonblank_reason)]
    reason: String,
}

#[derive(Debug, Args)]
pub(crate) struct ConvertArgs {
    #[arg(long, value_enum)]
    direction: ConversionDirection,
    /// USDC to sell for `to-usd`, or dollars in whole cents to spend for
    /// `to-usdc`.
    #[arg(long, value_parser = parse_positive_usdc)]
    amount: Positive<Usdc>,
    #[arg(long)]
    client_order_id: Option<Uuid>,
    #[arg(long, value_parser = nonblank_reason)]
    reason: String,
}

impl ConvertArgs {
    /// The order this conversion places. A `to-usdc` amount finer than whole
    /// cents is refused: `st0x-alpaca` floors a finer `BuyWithUsd` amount,
    /// which would spend less than the amount typed.
    fn conversion(&self) -> Result<ConversionOrder, ArgumentError> {
        match self.direction {
            ConversionDirection::ToUsd => Ok(ConversionOrder::SellUsdc(self.amount)),
            ConversionDirection::ToUsdc => {
                let dollars = self.amount.inner().inner();
                let (_, lossless) = dollars.to_fixed_decimal_lossy(USD_NOTIONAL_DECIMAL_PLACES)?;
                if !lossless {
                    return Err(ArgumentError::FinerThanWholeCents {
                        amount: self.amount,
                    });
                }
                Ok(ConversionOrder::BuyWithUsd(Positive::new(Usd::new(
                    dollars,
                ))?))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ConversionDirection {
    ToUsd,
    ToUsdc,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum QuoteFeed {
    /// Consolidated NBBO, fifteen minutes delayed.
    DelayedSip,
    /// Realtime indicative overnight feed (Blue Ocean derived).
    Overnight,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum Network {
    Base,
    Ethereum,
    Hyperevm,
    Robinhood,
    Binance,
}

impl From<Network> for TokenizationNetwork {
    fn from(network: Network) -> Self {
        match network {
            Network::Base => Self::Base,
            Network::Ethereum => Self::Ethereum,
            Network::Hyperevm => Self::HyperEvm,
            Network::Robinhood => Self::Robinhood,
            Network::Binance => Self::BnbSmartChain,
        }
    }
}

/// An argument refused before any sign in: by its parser, or by
/// [`Command::validate`] for a rule that spans arguments.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ArgumentError {
    #[error(transparent)]
    Float(#[from] FloatError),
    /// A [`NotPositive`] refusal of an amount, boxed: beside the value, its
    /// comparison failure carries a [`FloatError`], which would make every
    /// result this error rides in large.
    #[error(transparent)]
    NotPositive(Box<dyn StdError + Send + Sync>),
    /// `st0x-alpaca` floors an order quantity to
    /// [`ALPACA_MAX_DECIMAL_PLACES`], which would place an order for a
    /// quantity other than the one typed.
    #[error(
        "Alpaca takes at most {max} decimal places in an order quantity",
        max = ALPACA_MAX_DECIMAL_PLACES
    )]
    FinerThanSharePrecision,
    #[error(
        "invalid value '{amount}' for '--amount': --direction to-usdc spends dollars, which \
         Alpaca takes in whole cents only"
    )]
    FinerThanWholeCents { amount: Positive<Usdc> },
}

impl<T> From<NotPositive<T>> for ArgumentError
where
    NotPositive<T>: StdError + Send + Sync + 'static,
{
    fn from(error: NotPositive<T>) -> Self {
        Self::NotPositive(Box::new(error))
    }
}

fn parse_positive_shares(value: &str) -> Result<Positive<FractionalShares>, ArgumentError> {
    Ok(Positive::new(value.parse::<FractionalShares>()?)?)
}

/// Reads an order quantity, refusing one finer than Alpaca's share precision.
fn parse_order_quantity(value: &str) -> Result<Positive<FractionalShares>, ArgumentError> {
    let quantity = parse_positive_shares(value)?;
    let (_, lossless) = quantity
        .inner()
        .inner()
        .to_fixed_decimal_lossy(ALPACA_MAX_DECIMAL_PLACES)?;
    if lossless {
        Ok(quantity)
    } else {
        Err(ArgumentError::FinerThanSharePrecision)
    }
}

fn parse_positive_usdc(value: &str) -> Result<Positive<Usdc>, ArgumentError> {
    Ok(Positive::new(value.parse::<Usdc>()?)?)
}

impl Command {
    #[must_use]
    pub(crate) const fn tier(&self) -> Tier {
        match self {
            Self::Broker(
                BrokerCommand::Buy(_)
                | BrokerCommand::Sell(_)
                | BrokerCommand::Cancel { .. }
                | BrokerCommand::Convert(_)
                | BrokerCommand::Journal { .. },
            )
            | Self::Wallet(
                WalletCommand::Withdraw { .. }
                | WalletCommand::Whitelist { .. }
                | WalletCommand::WhitelistPatchTravelRule { .. }
                | WalletCommand::Unwhitelist { .. },
            )
            | Self::Tokenization(TokenizationCommand::Tokenize { .. }) => Tier::Write,
            Self::Broker(
                BrokerCommand::Account
                | BrokerCommand::Cash
                | BrokerCommand::Positions
                | BrokerCommand::PositionMark { .. }
                | BrokerCommand::Activities { .. }
                | BrokerCommand::MarketStatus
                | BrokerCommand::LatestTrade { .. }
                | BrokerCommand::Quote { .. }
                | BrokerCommand::Asset { .. }
                | BrokerCommand::OrderStatus { .. }
                | BrokerCommand::OrderFind { .. }
                | BrokerCommand::ConversionStatus { .. }
                | BrokerCommand::ConversionFind { .. },
            )
            | Self::Wallet(
                WalletCommand::Deposit { .. }
                | WalletCommand::WhitelistList
                | WalletCommand::Transfers { .. }
                | WalletCommand::Transfer { .. }
                | WalletCommand::FindDeposit { .. },
            )
            | Self::Tokenization(
                TokenizationCommand::TokenizationRequest { .. }
                | TokenizationCommand::MintStatus { .. }
                | TokenizationCommand::Redeem { .. }
                | TokenizationCommand::TokenizationRequests { .. },
            ) => Tier::Read,
        }
    }

    /// Refuses, before any sign in, an argument combination clap cannot
    /// express on its own, as the legacy `st0x-cli` `validate_command` does:
    /// a `to-usdc` conversion amount finer than whole cents.
    pub(crate) fn validate(&self) -> Result<(), ArgumentError> {
        let Self::Broker(BrokerCommand::Convert(args)) = self else {
            return Ok(());
        };
        args.conversion()?;
        Ok(())
    }
}

/// The key a mutation is sent under: what its outcome is looked up by and,
/// for an operation the gateway catalog marks resendable, what it is sent
/// again with. Each renders as the JSON field that carries it.
#[derive(Clone, Debug)]
pub(crate) enum Key {
    ClientOrder(ClientOrderId),
    BrokerOrder(Uuid),
    Operation(Uuid),
    IssuerRequest(IssuerRequestId),
}

impl Display for Key {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClientOrder(key) => write!(formatter, "clientOrderId={key}"),
            Self::BrokerOrder(key) => write!(formatter, "orderId={key}"),
            Self::Operation(key) => write!(formatter, "operationId={key}"),
            Self::IssuerRequest(key) => write!(formatter, "issuerRequestId={key}"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Failure {
    /// A gateway call failed. The message is each distinct message of the
    /// error chain, the hop's retry flags, the wait asked for, and the key of
    /// a mutation.
    #[error("{}", render_gateway(&**.source, .key.as_ref()))]
    Gateway {
        source: Box<dyn StdError + Send + Sync>,
        key: Option<Key>,
    },
    #[error("could not build the Alpaca gateway client: {0}")]
    Client(#[from] ClientError),
    #[error("could not build the HTTP client of the whitelist operations: {0}")]
    HttpClient(#[from] reqwest::Error),
    #[error("could not write the mutation key to stderr, so the mutation was not sent: {0}")]
    Notice(#[source] std::io::Error),
    #[error("could not encode the Alpaca gateway response as JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Argument(#[from] ArgumentError),
    #[error(
        "the overnight session (20:00-04:00 ET) accepts only limit orders; retry with \
         --limit-price and --extended-hours, or queue for the next regular close with the \
         legacy stox buy or stox sell and --time-in-force market-on-close"
    )]
    OvernightMarketOrder,
    #[error("overnight orders require extended_hours = true; retry with --extended-hours")]
    OvernightWithoutExtendedHours,
    #[error(
        "{symbol} is not fractionable, so Alpaca takes a market order for it in whole shares \
         only, and st0x-alpaca would floor --quantity {quantity}; retry with a whole quantity"
    )]
    FractionalMarketOrder {
        symbol: Symbol,
        quantity: Positive<FractionalShares>,
    },
}

impl Failure {
    /// Whether IAP, or the gateway's own identity check, refused the
    /// operator. Read from the failed call's error, never from the rendered
    /// message.
    #[must_use]
    pub(crate) fn is_access_denied(&self) -> bool {
        match self {
            Self::Gateway { source, .. } => denies_access(&**source),
            Self::Client(_)
            | Self::HttpClient(_)
            | Self::Notice(_)
            | Self::Json(_)
            | Self::Argument(_)
            | Self::OvernightMarketOrder
            | Self::OvernightWithoutExtendedHours
            | Self::FractionalMarketOrder { .. } => false,
        }
    }
}

impl From<BrokerError> for Failure {
    fn from(error: BrokerError) -> Self {
        Self::Gateway {
            source: Box::new(error),
            key: None,
        }
    }
}

impl From<WalletError> for Failure {
    fn from(error: WalletError) -> Self {
        Self::Gateway {
            source: Box::new(error),
            key: None,
        }
    }
}

impl From<TokenizationError> for Failure {
    fn from(error: TokenizationError) -> Self {
        Self::Gateway {
            source: Box::new(error),
            key: None,
        }
    }
}

/// Sends one operator command and returns the compact JSON value the entry
/// point prints. A mutation's key goes to `notice`, the entry point's stderr,
/// before the request leaves.
pub(crate) async fn execute(
    base: &Url,
    token: String,
    command: Command,
    notice: &mut (dyn Write + Send),
) -> Result<Value, Failure> {
    let tier = command.tier();
    let client = GatewayClient::new(base.as_str(), tier, StaticToken(token.clone()))?;

    match command {
        Command::Broker(command) => broker(&client.broker(), command, notice).await,
        Command::Wallet(command) => {
            let whitelist = || RawGateway::new(base.clone(), tier, token);
            wallet(&client.wallet(), whitelist, command, notice).await
        }
        Command::Tokenization(command) => tokenization(&client, command, notice).await,
    }
}

async fn broker(
    broker: &GatewayBroker<StaticToken>,
    command: BrokerCommand,
    notice: &mut (dyn Write + Send),
) -> Result<Value, Failure> {
    match command {
        BrokerCommand::Account => Ok(serde_json::to_value(broker.account_funds().await?)?),
        BrokerCommand::Cash => {
            let withdrawable = broker.withdrawable_cash_cents().await?;
            Ok(json!({ "withdrawableCashCents": withdrawable }))
        }
        BrokerCommand::Positions => Ok(serde_json::to_value(broker.fetch_inventory().await?)?),
        BrokerCommand::PositionMark { symbol } => {
            let mark = broker.fetch_position_mark(&symbol).await?;
            Ok(json!({ "symbol": symbol, "mark": mark }))
        }
        BrokerCommand::Activities {
            types,
            after,
            until,
        } => {
            let activities = broker
                .fetch_account_activities(&AccountActivitiesQuery {
                    activity_types: types,
                    after,
                    until,
                })
                .await?;
            Ok(json!({ "activities": activities }))
        }
        BrokerCommand::MarketStatus => {
            Ok(serde_json::to_value(broker.market_session_status().await?)?)
        }
        BrokerCommand::LatestTrade { symbol } => {
            let price = broker.fetch_latest_trade_price(&symbol).await?;
            Ok(json!({ "symbol": symbol, "price": price }))
        }
        BrokerCommand::Quote { symbol, feed } => quote(broker, symbol, feed).await,
        BrokerCommand::Asset { symbol } => Ok(serde_json::to_value(
            broker.get_asset_details(&symbol).await?,
        )?),
        BrokerCommand::Buy(args) => place_order(broker, args, Direction::Buy, notice).await,
        BrokerCommand::Sell(args) => place_order(broker, args, Direction::Sell, notice).await,
        BrokerCommand::Cancel { order_id, reason } => {
            let key = Key::BrokerOrder(order_id);
            announce(notice, Operation::OrdersCancel, &key)?;
            let outcome = broker
                .cancel_order(&order_id.to_string(), Some(&reason))
                .await
                .map_err(|error| Failure::Gateway {
                    source: Box::new(error),
                    key: Some(key),
                })?;
            Ok(json!({ "orderId": order_id, "outcome": outcome }))
        }
        BrokerCommand::OrderStatus { order_id } => Ok(serde_json::to_value(
            broker.get_order_status(&order_id.to_string()).await?,
        )?),
        BrokerCommand::OrderFind { client_order_id } => {
            let key = ClientOrderId::cli(client_order_id);
            let order = broker.get_order_by_client_order_id(&key).await?;
            Ok(json!({ "clientOrderId": key, "order": order }))
        }
        BrokerCommand::Convert(args) => convert(broker, args, notice).await,
        BrokerCommand::ConversionStatus { order_id } => Ok(serde_json::to_value(
            broker.get_conversion_order(order_id).await?,
        )?),
        BrokerCommand::ConversionFind { client_order_id } => {
            let key = ClientOrderId::cli(client_order_id);
            let order = broker.find_conversion_order(&key).await?;
            Ok(json!({ "clientOrderId": key, "order": order }))
        }
        BrokerCommand::Journal {
            counterparty,
            symbol,
            quantity,
            operation_id,
            reason,
        } => {
            let operation_id = operation_id.unwrap_or_else(Uuid::new_v4);
            let key = Key::Operation(operation_id);
            announce(notice, Operation::JournalsCreate, &key)?;
            let journal = broker
                .create_journal(counterparty, &symbol, quantity, operation_id, reason)
                .await
                .map_err(|error| Failure::Gateway {
                    source: Box::new(error),
                    key: Some(key),
                })?;
            Ok(json!({ "operationId": operation_id, "journal": journal }))
        }
    }
}

/// The wallet commands. The whitelist operations go through the
/// [`RawGateway`] `whitelist` builds, which only they need.
async fn wallet(
    wallet: &GatewayWallet<StaticToken>,
    whitelist: impl FnOnce() -> Result<RawGateway, Failure>,
    command: WalletCommand,
    notice: &mut (dyn Write + Send),
) -> Result<Value, Failure> {
    match command {
        WalletCommand::Deposit { asset, network } => {
            let address = wallet
                .get_wallet_address(
                    &TokenSymbol::new(asset.clone()),
                    &WalletNetwork::new(network.clone()),
                )
                .await?;
            Ok(json!({ "asset": asset, "network": network, "address": address }))
        }
        WalletCommand::Withdraw {
            amount,
            asset,
            address,
            operation_id,
            reason,
        } => {
            let operation_id = operation_id.unwrap_or_else(Uuid::new_v4);
            let key = Key::Operation(operation_id);
            announce(notice, Operation::WalletWithdraw, &key)?;
            let transfer = wallet
                .initiate_withdrawal(
                    amount,
                    &TokenSymbol::new(asset),
                    &address,
                    operation_id,
                    Some(&reason),
                )
                .await
                .map_err(|error| Failure::Gateway {
                    source: Box::new(error),
                    key: Some(key),
                })?;
            Ok(json!({ "operationId": operation_id, "transfer": transfer }))
        }
        WalletCommand::Whitelist {
            address,
            asset,
            operation_id,
            reason,
        } => {
            let operation_id = operation_id.unwrap_or_else(Uuid::new_v4);
            let key = Key::Operation(operation_id);
            let request = WhitelistCreateRequest {
                address,
                asset: TokenSymbol::new(asset),
                operation_id,
                reason: Some(reason),
            };
            let gateway = whitelist()?;
            announce(notice, Operation::WalletWhitelistCreate, &key)?;
            let entry: WhitelistEntry = gateway
                .send(Operation::WalletWhitelistCreate, &[], Some(&request))
                .await
                .map_err(|error| Failure::Gateway {
                    source: Box::new(error),
                    key: Some(key),
                })?;
            Ok(json!({ "operationId": operation_id, "entry": entry }))
        }
        WalletCommand::WhitelistList => {
            let answer: WhitelistResponse = whitelist()?
                .send::<(), _>(Operation::WalletWhitelist, &[], None)
                .await?;
            Ok(json!({ "entries": answer.entries }))
        }
        WalletCommand::WhitelistPatchTravelRule {
            operation_id,
            reason,
        } => {
            let operation_id = operation_id.unwrap_or_else(Uuid::new_v4);
            let key = Key::Operation(operation_id);
            let request = TravelRulePatchRequest {
                operation_id,
                reason: Some(reason),
            };
            let gateway = whitelist()?;
            announce(notice, Operation::WalletWhitelistPatchTravelRule, &key)?;
            let answer: WhitelistResponse = gateway
                .send(
                    Operation::WalletWhitelistPatchTravelRule,
                    &[],
                    Some(&request),
                )
                .await
                .map_err(|error| Failure::Gateway {
                    source: Box::new(error),
                    key: Some(key),
                })?;
            Ok(json!({ "operationId": operation_id, "entries": answer.entries }))
        }
        WalletCommand::Unwhitelist {
            address,
            operation_id,
            reason,
        } => {
            let operation_id = operation_id.unwrap_or_else(Uuid::new_v4);
            let key = Key::Operation(operation_id);
            let request = WhitelistRemoveRequest {
                operation_id,
                reason: Some(reason),
            };
            let gateway = whitelist()?;
            announce(notice, Operation::WalletWhitelistRemove, &key)?;
            let answer: WhitelistResponse = gateway
                .send(
                    Operation::WalletWhitelistRemove,
                    &[("address", address.to_string())],
                    Some(&request),
                )
                .await
                .map_err(|error| Failure::Gateway {
                    source: Box::new(error),
                    key: Some(key),
                })?;
            Ok(json!({ "operationId": operation_id, "entries": answer.entries }))
        }
        WalletCommand::Transfers { pending } => {
            let mut transfers = wallet.list_all_transfers().await?;
            if pending {
                transfers.retain(|transfer| transfer.status.is_pending());
            }
            Ok(json!({ "transfers": transfers }))
        }
        WalletCommand::Transfer { transfer_id } => {
            let (transfer, reported_fees) = wallet
                .get_transfer_with_fees(&AlpacaTransferId::from(transfer_id))
                .await?;
            Ok(json!({ "transfer": transfer, "reportedFees": reported_fees }))
        }
        WalletCommand::FindDeposit { tx_hash } => {
            let deposit = wallet.find_deposit_by_tx_hash(&tx_hash).await?;
            Ok(json!({ "deposit": deposit }))
        }
    }
}

async fn tokenization(
    client: &GatewayClient<StaticToken>,
    command: TokenizationCommand,
    notice: &mut (dyn Write + Send),
) -> Result<Value, Failure> {
    match command {
        TokenizationCommand::Tokenize {
            symbol,
            quantity,
            recipient,
            network,
            issuer_request_id,
            reason,
        } => {
            let issuer_request_id = IssuerRequestId(issuer_request_id.unwrap_or_else(Uuid::new_v4));
            let key = Key::IssuerRequest(issuer_request_id.clone());
            announce(notice, Operation::TokenizationMint, &key)?;
            let request = client
                .tokenization(network.into())
                .request_mint(
                    symbol,
                    quantity,
                    recipient,
                    issuer_request_id.clone(),
                    Some(&reason),
                )
                .await
                .map_err(|error| Failure::Gateway {
                    source: Box::new(error),
                    key: Some(key),
                })?;
            Ok(json!({ "issuerRequestId": issuer_request_id, "request": request }))
        }
        TokenizationCommand::TokenizationRequest {
            request_id,
            network,
        } => Ok(serde_json::to_value(
            client
                .tokenization(network.into())
                .get_request(&request_id)
                .await?,
        )?),
        TokenizationCommand::MintStatus {
            issuer_request_id,
            network,
        } => {
            let request = client
                .tokenization(network.into())
                .find_mint_by_issuer_request_id(&issuer_request_id)
                .await?;
            Ok(json!({ "issuerRequestId": issuer_request_id, "request": request }))
        }
        TokenizationCommand::Redeem { tx_hash, network } => {
            let request = client
                .tokenization(network.into())
                .find_redemption_by_tx(&tx_hash)
                .await?;
            Ok(json!({ "transactionHash": tx_hash, "request": request }))
        }
        TokenizationCommand::TokenizationRequests { network, pending } => {
            let tokenization = client.tokenization(network.into());
            let requests = if pending {
                tokenization.list_pending_requests().await
            } else {
                tokenization.list_requests().await
            }?;
            Ok(json!({ "requests": requests }))
        }
    }
}

async fn quote(
    broker: &GatewayBroker<StaticToken>,
    symbol: Symbol,
    feed: QuoteFeed,
) -> Result<Value, Failure> {
    match feed {
        QuoteFeed::DelayedSip => {
            let quote = broker.fetch_latest_quote(&symbol).await?;
            Ok(json!({
                "symbol": symbol,
                "feed": "delayed_sip",
                "bid": quote.bid(),
                "ask": quote.ask(),
            }))
        }
        QuoteFeed::Overnight => {
            let quote = broker.fetch_latest_overnight_quote(&symbol).await?;
            Ok(json!({
                "symbol": symbol,
                "feed": "overnight",
                "bid": quote.quote.bid(),
                "ask": quote.quote.ask(),
                "at": quote.at,
            }))
        }
    }
}

/// Places a buy or sell. Before its key is announced, it reads the market
/// session and, for a market order, the asset, and refuses an order the
/// legacy `stox buy` and `stox sell` refuse for the session, or one
/// `st0x-alpaca` would place for a quantity other than the one typed. An
/// exact limit order is placed for the quantity typed, so only the session
/// rule applies to it.
async fn place_order(
    broker: &GatewayBroker<StaticToken>,
    args: OrderArgs,
    direction: Direction,
    notice: &mut (dyn Write + Send),
) -> Result<Value, Failure> {
    check_session(broker.market_session().await?, &args)?;
    if args.limit_price.is_none() {
        check_market_order_shares(broker.get_asset_details(&args.symbol).await?, &args)?;
    }

    let OrderArgs {
        symbol,
        quantity,
        limit_price,
        extended_hours,
        client_order_id,
        reason,
    } = args;
    let client_order_id = ClientOrderId::cli(client_order_id.unwrap_or_else(Uuid::new_v4));
    let key = Key::ClientOrder(client_order_id.clone());
    let operation = if limit_price.is_some() {
        Operation::OrdersPlaceExactLimit
    } else {
        Operation::OrdersPlaceMarket
    };
    announce(notice, operation, &key)?;
    let order = match limit_price {
        Some(limit_price) => {
            broker
                .place_alpaca_limit_order(
                    AlpacaLimitOrder {
                        symbol,
                        shares: quantity,
                        direction,
                        limit_price,
                        extended_hours,
                        client_order_id: client_order_id.clone(),
                    },
                    reason,
                )
                .await
        }
        None => {
            broker
                .place_market_order(
                    MarketOrder {
                        symbol,
                        shares: quantity,
                        direction,
                        client_order_id: client_order_id.clone(),
                    },
                    Some(&reason),
                )
                .await
        }
    }
    .map_err(|error| Failure::Gateway {
        source: Box::new(error),
        key: Some(key),
    })?;
    Ok(json!({ "clientOrderId": client_order_id, "order": order }))
}

/// The session rule of the legacy `stox buy` and `stox sell`
/// (`validate_order_kind_for_session`): the overnight session takes only
/// limit orders flagged extended hours, so any other order is refused before
/// it is sent, with the contract spelled out, rather than left to Alpaca's
/// rejection. Exhaustive on the session, so a new session must decide its
/// order contract here.
const fn check_session(session: MarketSession, order: &OrderArgs) -> Result<(), Failure> {
    match (session, order.limit_price.is_some(), order.extended_hours) {
        (MarketSession::Regular | MarketSession::Extended | MarketSession::Closed, _, _)
        | (MarketSession::Overnight, true, true) => Ok(()),
        (MarketSession::Overnight, false, _) => Err(Failure::OvernightMarketOrder),
        (MarketSession::Overnight, true, false) => Err(Failure::OvernightWithoutExtendedHours),
    }
}

/// The share grid `st0x-alpaca` sizes a market order on
/// (`prepare_counter_trade_shares`, without extended hours, as
/// `place_market_order` applies it): nine decimals for a fractionable asset,
/// whole shares otherwise, an asset whose metadata omits `fractionable`
/// included. A quantity off that grid would be floored to a smaller order,
/// or refused below one share, so it is refused here instead.
fn check_market_order_shares(asset: AssetDetails, order: &OrderArgs) -> Result<(), Failure> {
    if asset.fractionable == Some(true)
        || order
            .quantity
            .inner()
            .is_whole()
            .map_err(ArgumentError::from)?
    {
        return Ok(());
    }
    Err(Failure::FractionalMarketOrder {
        symbol: order.symbol.clone(),
        quantity: order.quantity,
    })
}

/// Places a conversion and waits until it settles, as the legacy
/// `alpaca-convert` does: `st0x-alpaca`'s conversion loop polls the order to
/// a terminal state and cancels the remainder past its deadline.
async fn convert(
    broker: &GatewayBroker<StaticToken>,
    args: ConvertArgs,
    notice: &mut (dyn Write + Send),
) -> Result<Value, Failure> {
    let conversion = args.conversion()?;
    let ConvertArgs {
        client_order_id,
        reason,
        ..
    } = args;
    let client_order_id = ClientOrderId::cli(client_order_id.unwrap_or_else(Uuid::new_v4));
    let key = Key::ClientOrder(client_order_id.clone());
    announce(notice, Operation::ConversionsSubmit, &key)?;
    let order = broker
        .convert_usdc_usd(conversion, &client_order_id, Some(&reason))
        .await
        .map_err(|error| Failure::Gateway {
            source: Box::new(error),
            key: Some(key),
        })?;
    Ok(json!({ "clientOrderId": client_order_id, "order": order }))
}

/// Writes a mutation's key, and whether resending it with that key is safe,
/// to `notice` in one line before the request leaves, so a run cut short by a
/// timeout or an interrupt still shows what to look the outcome up by.
fn announce(
    notice: &mut (dyn Write + Send),
    operation: Operation,
    key: &Key,
) -> Result<(), Failure> {
    writeln!(
        notice,
        "{key}; retryableWithSameKey={}",
        operation.resendable_with_same_key()
    )
    .and_then(|()| notice.flush())
    .map_err(Failure::Notice)
}

fn render_gateway(error: &(dyn StdError + 'static), key: Option<&Key>) -> String {
    let mut messages = Vec::new();
    let mut hop = None;
    let mut wait = None;
    for source in std::iter::successors(Some(error), |&source| source.source()) {
        let message = source.to_string();
        if messages.last() != Some(&message) {
            messages.push(message);
        }
        hop = hop.or_else(|| gateway_hop(source));
        wait = wait.or_else(|| retry_after(source));
    }
    let mut rendered = messages.join(": ");
    use std::fmt::Write as _;
    if let Some(hop) = hop {
        let _ = write!(
            rendered,
            "; retryable={}; outcomeUnknown={}; retryableWithSameKey={}",
            hop.retryable, hop.outcome_unknown, hop.retryable_with_same_key
        );
    }
    if let Some(wait) = wait {
        let _ = write!(rendered, "; retryAfterSecs={}", wait.as_secs());
    }
    if let Some(key) = key {
        let _ = write!(rendered, "; {key}");
    }
    rendered
}

fn gateway_hop<'a>(error: &'a (dyn StdError + 'static)) -> Option<&'a GatewayHopError> {
    if let Some(BrokerError::Gateway(hop)) = error.downcast_ref::<BrokerError>() {
        return Some(hop);
    }
    if let Some(WalletError::Gateway(hop)) = error.downcast_ref::<WalletError>() {
        return Some(hop);
    }
    if let Some(TokenizationError::Gateway(hop)) = error.downcast_ref::<TokenizationError>() {
        return Some(hop);
    }
    error.downcast_ref::<GatewayHopError>()
}

/// The wait an error asks for: the hold a gateway hop relays, or the
/// `Retry-After` of a 429 the gateway relays as Alpaca's own answer.
fn retry_after(error: &(dyn StdError + 'static)) -> Option<Duration> {
    error
        .downcast_ref::<BrokerError>()
        .map(BrokerError::backpressure)
        .or_else(|| {
            error
                .downcast_ref::<WalletError>()
                .map(WalletError::backpressure)
        })
        .or_else(|| {
            error
                .downcast_ref::<TokenizationError>()
                .map(TokenizationError::backpressure)
        })
        .or_else(|| {
            error
                .downcast_ref::<GatewayHopError>()
                .map(GatewayHopError::backpressure)
        })
        .flatten()
        .and_then(|backpressure| backpressure.retry_after)
}

/// Whether IAP, or the gateway's own identity check, refused the operator.
/// Only a hop carries such an answer, in its message as `ClientError`'s
/// Display writes it: `unexpected answer {status}: ...` for an answer that is
/// not a gateway error body, which in front of the gateway is IAP's, and
/// `{status} {code:?}: ...` for a gateway error body. IAP refuses with 401 or
/// 403, or redirects to its sign in. The gateway refuses an identity with
/// `401 Unauthenticated`; its 403s refuse the operation, not the operator:
/// `Forbidden` for a destination outside the deployment's pinned lists, and
/// `CapabilityDisabled` for an operation the deployment switched off. An
/// Alpaca status the gateway relays, such as a 403 order rejection, is not a
/// hop.
fn denies_access(error: &(dyn StdError + 'static)) -> bool {
    let Some(hop) =
        std::iter::successors(Some(error), |&source| source.source()).find_map(gateway_hop)
    else {
        return false;
    };
    let Some(answer) = hop.message.strip_prefix("unexpected answer ") else {
        let unauthenticated = ErrorCode::Unauthenticated;
        return hop.message.starts_with(&format!(
            "{} {unauthenticated:?}:",
            unauthenticated.status()
        ));
    };
    answer
        .find(|character: char| !character.is_ascii_digit())
        .and_then(|digits| answer[..digits].parse::<u16>().ok())
        .is_some_and(|status| status == 401 || status == 403 || (300..400).contains(&status))
}

/// Sends the whitelist operations. `GatewayWallet` has no whitelist methods
/// at this `st0x-alpaca-gateway-api` rev, and `GatewayClient::send` is
/// private to that crate, so this mirrors that `send` and its `exchange`:
/// one bound over the whole exchange, body included, no redirect followed,
/// the same connect bound, and every failure read as the adapter reads a
/// wallet call's (`wallet_error`), so these errors render, classify, and
/// decide access like every other command's. It goes once st0x.alpaca serves
/// the whitelist operations.
struct RawGateway {
    base: Url,
    tier: Tier,
    token: String,
    http: reqwest::Client,
}

/// How a whitelist exchange ended without the answer it asked for: a
/// `ClientError` as `GatewayClient`'s exchange builds it, or a transport
/// failure, which `ClientError::Transport` holds as the gateway client's own
/// reqwest error type, a major version this client's reqwest cannot build.
enum Unanswered {
    Client(ClientError),
    Transport(reqwest::Error),
}

impl From<ClientError> for Unanswered {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl RawGateway {
    fn new(base: Url, tier: Tier, token: String) -> Result<Self, Failure> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .build()?;
        Ok(Self {
            base,
            tier,
            token,
            http,
        })
    }

    /// Sends one operation, bounded as a whole, body read included, by the
    /// operation's deadline plus [`CLIENT_DEADLINE_MARGIN`].
    async fn send<Request, Response>(
        &self,
        operation: Operation,
        params: &[(&'static str, String)],
        body: Option<&Request>,
    ) -> Result<Response, WalletError>
    where
        Request: Serialize + Sync + ?Sized,
        Response: DeserializeOwned,
    {
        let bound = operation.deadline() + CLIENT_DEADLINE_MARGIN;
        tokio::time::timeout(bound, self.exchange(operation, params, body))
            .await
            .unwrap_or_else(|_| Err(ClientError::Timeout(bound).into()))
            .map_err(|error| wallet_error(operation, error))
    }

    async fn exchange<Request, Response>(
        &self,
        operation: Operation,
        params: &[(&'static str, String)],
        body: Option<&Request>,
    ) -> Result<Response, Unanswered>
    where
        Request: Serialize + Sync + ?Sized,
        Response: DeserializeOwned,
    {
        let url = self.url(operation, params)?;
        let request = match operation.method() {
            Method::Get => self.http.get(url),
            Method::Post => self.http.post(url),
        }
        .bearer_auth(&self.token);
        let request = match body {
            Some(body) => request.json(body),
            None => request,
        };

        let response = request.send().await.map_err(Unanswered::Transport)?;
        let status = response.status();
        let bytes = response.bytes().await.map_err(Unanswered::Transport)?;

        if status.is_success() {
            return serde_json::from_slice(&bytes).map_err(|error| {
                ClientError::Unexpected {
                    status: status.as_u16(),
                    detail: error.to_string(),
                }
                .into()
            });
        }

        Err(serde_json::from_slice::<ErrorBody>(&bytes)
            .map_or_else(
                |_| ClientError::Unexpected {
                    status: status.as_u16(),
                    detail: String::from_utf8_lossy(&bytes).chars().take(512).collect(),
                },
                |body| ClientError::Gateway {
                    status: status.as_u16(),
                    body,
                },
            )
            .into())
    }

    /// Builds the URL as `GatewayClient` does: the base URL's path, the tier
    /// prefix, and the operation's path, each parameter one path segment,
    /// refusing an empty or dot segment value, even percent encoded.
    fn url(
        &self,
        operation: Operation,
        params: &[(&'static str, String)],
    ) -> Result<Url, ClientError> {
        let mut segments: Vec<&str> = self
            .tier
            .prefix()
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect();
        for segment in operation
            .path()
            .split('/')
            .filter(|segment| !segment.is_empty())
        {
            let value = match segment
                .strip_prefix('{')
                .and_then(|rest| rest.strip_suffix('}'))
            {
                Some(name) => {
                    let value = params
                        .iter()
                        .find(|(param, _)| *param == name)
                        .map(|(_, value)| value.as_str())
                        .ok_or(ClientError::MissingParameter(name))?;
                    let decoded = value.to_ascii_lowercase().replace("%2e", ".");
                    if value.is_empty() || decoded == "." || decoded == ".." {
                        return Err(ClientError::InvalidParameter {
                            name,
                            value: value.to_owned(),
                        });
                    }
                    value
                }
                None => segment,
            };
            segments.push(value);
        }

        let mut url = self.base.clone();
        // `execute` builds a `GatewayClient` on the same base URL first, and
        // its origin check guarantees a host, so the URL can always be a base
        // and `path_segments_mut` cannot fail.
        if let Ok(mut path) = url.path_segments_mut() {
            path.pop_if_empty().extend(segments);
        }
        Ok(url)
    }
}

/// Reads a failed whitelist exchange as the gateway client's adapter reads a
/// wallet call without a typed rejection: `answer`, then `wallet_error`, in
/// `st0x-alpaca-gateway-api` `client/adapter.rs`. An exchange that may have
/// left is, for a mutation, an unknown outcome that only a resendable
/// operation may send again with the same key.
fn wallet_error(operation: Operation, error: Unanswered) -> WalletError {
    let (message, may_have_left, retryable) = match error {
        Unanswered::Client(ClientError::Gateway { status, body }) => {
            return answered(status, body);
        }
        Unanswered::Client(error) => {
            let (may_have_left, retryable) = reach_and_retry(&error);
            (error.to_string(), may_have_left, retryable)
        }
        // `ClientError::Transport`'s message, and `reach_and_retry`'s reading
        // of it: a request that never built fails the same way again, and a
        // connection that never opened carried nothing and can open later.
        Unanswered::Transport(source) => (
            format!("no answer from the gateway: {source}"),
            !(source.is_builder() || source.is_connect()),
            !source.is_builder(),
        ),
    };

    let hop = GatewayHopError::transport(message);
    WalletError::Gateway(if may_have_left && operation.mutates() {
        GatewayHopError {
            retryable: false,
            outcome_unknown: true,
            retryable_with_same_key: operation.resendable_with_same_key(),
            ..hop
        }
    } else {
        GatewayHopError { retryable, ..hop }
    })
}

/// The adapter's `reach_and_retry`: whether a call that got no gateway answer
/// may have reached the gateway, and so Alpaca, and whether a later call can
/// succeed. `RawGateway` builds no `Token`, `TokenStatus`, `Origin`,
/// `HttpClient`, or `Transport` error, but the match stays exhaustive, so a
/// new `ClientError` variant forces a reading here too.
fn reach_and_retry(error: &ClientError) -> (bool, bool) {
    match error {
        ClientError::Gateway { body, .. } => (true, body.retryable),
        ClientError::Transport(source) if source.is_builder() => (false, false),
        ClientError::Transport(source) if source.is_connect() => (false, true),
        ClientError::Transport(_) | ClientError::Timeout(_) => (true, true),
        ClientError::Unexpected { status, .. } => (true, clears_on_its_own(*status)),
        ClientError::Token(_) => (false, true),
        ClientError::TokenStatus(status) => (false, clears_on_its_own(*status)),
        ClientError::Origin(_)
        | ClientError::HttpClient(_)
        | ClientError::MissingParameter(_)
        | ClientError::InvalidParameter { .. } => (false, false),
    }
}

/// The adapter's `clears_on_its_own`: whether an HTTP status in front of the
/// gateway can clear on its own.
const fn clears_on_its_own(status: u16) -> bool {
    status == 408 || status == 429 || status >= 500
}

/// The adapter's `answered`, for a call without a typed rejection: Alpaca's
/// own answer, a rate limit included, is relayed with its status, and every
/// other error body becomes a hop carrying the body's classification.
fn answered(status: u16, body: ErrorBody) -> WalletError {
    let alpaca = body
        .alpaca_status
        .and_then(|alpaca| StatusCode::from_u16(alpaca).ok());
    let relayed = match body.code {
        ErrorCode::Backpressure => Some(alpaca.unwrap_or(StatusCode::TOO_MANY_REQUESTS)),
        ErrorCode::UpstreamTransient | ErrorCode::Rejected => alpaca,
        ErrorCode::InvalidRequest
        | ErrorCode::Unauthenticated
        | ErrorCode::Forbidden
        | ErrorCode::CapabilityDisabled
        | ErrorCode::UnknownOperation
        | ErrorCode::Unavailable
        | ErrorCode::NotReady
        | ErrorCode::OutcomeUnknown => None,
    };

    if let Some(alpaca) = relayed {
        return WalletError::ApiError {
            status: alpaca,
            message: body.message,
            retry_after: body.retry_after_secs.map(Duration::from_secs),
        };
    }
    let retryable = body.retryable && body.code != ErrorCode::Rejected;
    let outcome_unknown =
        body.code == ErrorCode::OutcomeUnknown || body.outcome == Some(Outcome::Unknown);
    let retryable_with_same_key = body.retryable_with_same_key;
    let retry_after = body.retry_after_secs.map(Duration::from_secs);
    WalletError::Gateway(GatewayHopError {
        message: ClientError::Gateway { status, body }.to_string(),
        retryable,
        outcome_unknown,
        retryable_with_same_key,
        retry_after,
    })
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::time::Duration;

    use clap::Parser;
    use clap::error::ErrorKind;
    use httpmock::prelude::*;
    use st0x_alpaca::broker::AlpacaBrokerApiError;

    use super::*;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        command: Command,
    }

    fn parsed(arguments: &[&str]) -> Result<Command, clap::Error> {
        TestCli::try_parse_from(std::iter::once("client").chain(arguments.iter().copied()))
            .map(|cli| cli.command)
    }

    fn route(tier: Tier, operation: Operation) -> String {
        format!("{}{}", tier.prefix(), operation.path())
    }

    /// Runs `command` against `server` with the operator token, discarding
    /// what it writes to stderr.
    async fn run(server: &MockServer, command: Command) -> Result<Value, Failure> {
        execute(
            &Url::parse(&server.base_url()).unwrap(),
            "operator-token".to_owned(),
            command,
            &mut std::io::sink(),
        )
        .await
    }

    /// The reads a buy or sell makes before it is sent: the market session
    /// and, for a market order, the asset with `fractionable` as given.
    fn preflight(server: &MockServer, session: &str, fractionable: Option<bool>) {
        server.mock(|when, then| {
            when.method(GET)
                .path(route(Tier::Write, Operation::MarketSession));
            then.status(200).json_body(json!({ "session": session }));
        });
        server.mock(|when, then| {
            when.method(GET)
                .path(format!("{}/assets/COIN", Tier::Write.prefix()));
            then.status(200).json_body(json!({
                "status": "active",
                "tradable": true,
                "fractionable": fractionable,
                "fractionalEhEnabled": null,
                "overnightTradable": true,
                "overnightHalted": false
            }));
        });
    }

    /// The entry point's stderr in a test: what `execute` wrote, and how many
    /// requests `mock` had received at each write.
    struct Notice<'a> {
        mock: &'a httpmock::Mock<'a>,
        written: Vec<u8>,
        calls_at_write: Vec<usize>,
    }

    impl Write for Notice<'_> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.calls_at_write.push(self.mock.calls());
            self.written.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn mutation_reason_is_required_and_nonblank() {
        let missing = parsed(&["cancel", "11111111-1111-4111-8111-111111111111"]).unwrap_err();
        assert_eq!(missing.kind(), ErrorKind::MissingRequiredArgument);

        let blank = parsed(&[
            "cancel",
            "11111111-1111-4111-8111-111111111111",
            "--reason",
            " ",
        ])
        .unwrap_err();
        assert_eq!(blank.kind(), ErrorKind::ValueValidation);
    }

    #[test]
    fn no_command_accepts_an_account_identifier() {
        let error = parsed(&[
            "account",
            "--account-id",
            "11111111-1111-4111-8111-111111111111",
        ])
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnknownArgument);
    }

    #[test]
    fn gateway_retry_metadata_is_preserved_for_operator_decisions() {
        let source = AlpacaBrokerApiError::Gateway(GatewayHopError {
            message: "504 outcome_unknown".to_owned(),
            retryable: false,
            outcome_unknown: true,
            retryable_with_same_key: true,
            retry_after: Some(Duration::from_secs(12)),
        });
        let rendered = Failure::from(source).to_string();
        assert!(rendered.contains("outcomeUnknown=true"), "{rendered}");
        assert!(rendered.contains("retryableWithSameKey=true"), "{rendered}");
        assert!(rendered.contains("retryAfterSecs=12"), "{rendered}");
    }

    #[test]
    fn order_quantity_finer_than_alpaca_share_precision_is_refused() {
        let buy = |quantity| {
            parsed(&[
                "buy",
                "--symbol",
                "COIN",
                "--quantity",
                quantity,
                "--reason",
                "operator purchase",
            ])
        };

        buy("1.000000001").unwrap();
        let error = buy("1.0000000001").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert!(
            error.to_string().contains("at most 9 decimal places"),
            "{error}"
        );
    }

    /// The whole cents rule spans `--direction` and `--amount`, so the
    /// arguments parse and the refusal comes from `validate`, which the entry
    /// point runs before any sign in.
    #[test]
    fn to_usdc_amount_finer_than_whole_cents_is_refused_before_sign_in() {
        let convert = |direction, amount| {
            parsed(&[
                "convert",
                "--direction",
                direction,
                "--amount",
                amount,
                "--reason",
                "fund withdrawal",
            ])
            .unwrap()
        };

        let error = convert("to-usdc", "1.009").validate().unwrap_err();
        assert!(
            matches!(error, ArgumentError::FinerThanWholeCents { .. }),
            "{error:?}"
        );
        assert!(error.to_string().contains("whole cents"), "{error}");

        let Command::Broker(BrokerCommand::Convert(whole_cents)) = convert("to-usdc", "1.01")
        else {
            panic!("convert parses as a broker command");
        };
        let ConversionOrder::BuyWithUsd(dollars) = whole_cents.conversion().unwrap() else {
            panic!("a to-usdc conversion spends dollars");
        };
        assert_eq!(dollars.inner().to_string(), "1.01");

        let to_usd = convert("to-usd", "1.009");
        to_usd.validate().unwrap();
        let Command::Broker(BrokerCommand::Convert(to_usd)) = to_usd else {
            panic!("convert parses as a broker command");
        };
        let ConversionOrder::SellUsdc(usdc) = to_usd.conversion().unwrap() else {
            panic!("a to-usd conversion sells USDC");
        };
        assert_eq!(usdc.inner().to_string(), "1.009");
    }

    #[tokio::test]
    async fn account_read_uses_the_read_tier_and_returns_contract_json() {
        let server = MockServer::start();
        let request = server.mock(|when, then| {
            when.method(GET)
                .path(route(Tier::Read, Operation::AccountFunds))
                .header("authorization", "Bearer operator-token");
            then.status(200).json_body(json!({
                "balance": 100,
                "buyingPower": 80,
                "withdrawable": 70
            }));
        });

        let output = run(&server, Command::Broker(BrokerCommand::Account))
            .await
            .unwrap();

        request.assert();
        assert_eq!(
            output,
            json!({ "balance": 100, "buyingPower": 80, "withdrawable": 70 })
        );
    }

    /// `--feed delayed-sip` reads the latest quote endpoint, which the
    /// pinned library serves from Alpaca's fifteen minute delayed SIP feed,
    /// and labels it so; the default feed is the overnight one.
    #[tokio::test]
    async fn quote_labels_the_feed_it_read() {
        let cases: [(&[&str], &str, Value, &str); 2] = [
            (
                &["quote", "--symbol", "COIN", "--feed", "delayed-sip"],
                "latest-quote",
                json!({ "bid": "100.01", "ask": "100.02" }),
                "delayed_sip",
            ),
            (
                &["quote", "--symbol", "COIN"],
                "latest-overnight-quote",
                json!({ "bid": "100.01", "ask": "100.02", "at": "2026-10-06T02:00:00Z" }),
                "overnight",
            ),
        ];

        for (arguments, endpoint, body, feed) in cases {
            let server = MockServer::start();
            let request = server.mock(|when, then| {
                when.method(GET).path(format!(
                    "{}/market/stocks/COIN/{endpoint}",
                    Tier::Read.prefix()
                ));
                then.status(200).json_body(body);
            });

            let output = run(&server, parsed(arguments).unwrap()).await.unwrap();

            request.assert();
            assert_eq!(output["symbol"], "COIN", "{arguments:?}");
            assert_eq!(output["feed"], feed, "{arguments:?}");
        }
    }

    #[tokio::test]
    async fn whitelist_patch_uses_the_write_tier_and_echoes_its_operation_id() {
        let server = MockServer::start();
        let operation_id = Uuid::new_v4();
        let request = server.mock(|when, then| {
            when.method(POST)
                .path(route(
                    Tier::Write,
                    Operation::WalletWhitelistPatchTravelRule,
                ))
                .header("authorization", "Bearer operator-token")
                .json_body(json!({
                    "operationId": operation_id,
                    "reason": "Apply the configured beneficiary"
                }));
            then.status(200).json_body(json!({ "entries": [] }));
        });

        let output = run(
            &server,
            Command::Wallet(WalletCommand::WhitelistPatchTravelRule {
                operation_id: Some(operation_id),
                reason: "Apply the configured beneficiary".to_owned(),
            }),
        )
        .await
        .unwrap();

        request.assert();
        assert_eq!(
            output,
            json!({ "operationId": operation_id, "entries": [] })
        );
    }

    /// The gateway answers a whitelist create with the one entry it created.
    /// The generated key reaches stderr before the request leaves, and it is
    /// the key the result carries.
    #[tokio::test]
    async fn whitelist_create_announces_its_key_before_sending_and_returns_the_entry() {
        let server = MockServer::start();
        let request = server.mock(|when, then| {
            when.method(POST)
                .path(route(Tier::Write, Operation::WalletWhitelistCreate))
                .header("authorization", "Bearer operator-token");
            then.status(200).json_body(json!({
                "id": "entry-1",
                "address": "0x1111111111111111111111111111111111111111",
                "asset": "USDC",
                "chain": "ethereum",
                "status": "PENDING",
                "created_at": "2026-10-12T00:00:00Z"
            }));
        });
        let mut notice = Notice {
            mock: &request,
            written: Vec::new(),
            calls_at_write: Vec::new(),
        };

        let output = execute(
            &Url::parse(&server.base_url()).unwrap(),
            "operator-token".to_owned(),
            Command::Wallet(WalletCommand::Whitelist {
                address: Address::repeat_byte(0x11),
                asset: "USDC".to_owned(),
                operation_id: None,
                reason: "approve treasury wallet".to_owned(),
            }),
            &mut notice,
        )
        .await
        .unwrap();

        request.assert();
        assert_eq!(output["entry"]["id"], "entry-1");
        let line = String::from_utf8(notice.written).unwrap();
        let operation_id = line
            .strip_prefix("operationId=")
            .and_then(|rest| rest.strip_suffix("; retryableWithSameKey=false\n"))
            .unwrap();
        assert_eq!(output["operationId"], operation_id);
        assert!(
            notice.calls_at_write.iter().all(|calls| *calls == 0),
            "the key reached stderr after the request: {:?}",
            notice.calls_at_write
        );
    }

    /// A 2xx that does not decode means a mutation may have been applied:
    /// unknown outcome, and whitelist create is not resendable by key.
    #[tokio::test]
    async fn undecodable_whitelist_create_answer_is_an_unknown_outcome_with_its_key() {
        let server = MockServer::start();
        let operation_id = Uuid::new_v4();
        server.mock(|when, then| {
            when.method(POST)
                .path(route(Tier::Write, Operation::WalletWhitelistCreate));
            then.status(200).body("created");
        });

        let failure = run(
            &server,
            Command::Wallet(WalletCommand::Whitelist {
                address: Address::repeat_byte(0x11),
                asset: "USDC".to_owned(),
                operation_id: Some(operation_id),
                reason: "approve treasury wallet".to_owned(),
            }),
        )
        .await
        .unwrap_err();

        let rendered = failure.to_string();
        for expected in [
            "unexpected answer 200",
            "retryable=false",
            "outcomeUnknown=true",
            "retryableWithSameKey=false",
            &format!("operationId={operation_id}"),
        ] {
            assert!(rendered.contains(expected), "{expected}: {rendered}");
        }
        assert!(!failure.is_access_denied());
    }

    /// A gateway that answers a whitelist create with a status line and
    /// headers promising a body, and then never sends the body. The
    /// exchange's bound covers the body read, so the create ends at that
    /// bound as an unknown outcome carrying its key. The answer is served
    /// from a blocking task, which holds the paused clock until the headers
    /// are written, so the bound can only run out once the body is awaited.
    #[tokio::test(start_paused = true)]
    async fn whitelist_create_whose_answer_body_never_arrives_ends_at_its_bound() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let _gateway = tokio::task::spawn_blocking(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 4096];
            let read = stream.read(&mut request).unwrap();
            assert_ne!(read, 0, "the request arrives before the answer leaves");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                      content-length: 64\r\n\r\n",
                )
                .unwrap();
            stream
        });
        let operation_id = Uuid::new_v4();
        let bound = Operation::WalletWhitelistCreate.deadline() + CLIENT_DEADLINE_MARGIN;

        let failure = tokio::time::timeout(
            bound * 2,
            execute(
                &base,
                "operator-token".to_owned(),
                Command::Wallet(WalletCommand::Whitelist {
                    address: Address::repeat_byte(0x11),
                    asset: "USDC".to_owned(),
                    operation_id: Some(operation_id),
                    reason: "approve treasury wallet".to_owned(),
                }),
                &mut std::io::sink(),
            ),
        )
        .await
        .expect("the exchange's bound covers the answer's body")
        .unwrap_err();

        let rendered = failure.to_string();
        for expected in [
            format!("no answer from the gateway within {bound:?}"),
            "outcomeUnknown=true".to_owned(),
            "retryableWithSameKey=false".to_owned(),
            format!("operationId={operation_id}"),
        ] {
            assert!(rendered.contains(&expected), "{expected}: {rendered}");
        }
    }

    /// An intermediary's error page says nothing about whether the removal
    /// reached Alpaca. Removal is resendable with the same key, and the error
    /// carries that key.
    #[tokio::test]
    async fn unwhitelist_without_a_gateway_answer_keeps_its_key_and_same_key_retry() {
        let server = MockServer::start();
        let operation_id = Uuid::new_v4();
        let address = Address::repeat_byte(0x22);
        server.mock(|when, then| {
            when.method(POST).path(format!(
                "{}/wallet/whitelist/{address}/remove",
                Tier::Write.prefix()
            ));
            then.status(502).body("<html>bad gateway</html>");
        });

        let failure = run(
            &server,
            Command::Wallet(WalletCommand::Unwhitelist {
                address,
                operation_id: Some(operation_id),
                reason: "remove retired wallet".to_owned(),
            }),
        )
        .await
        .unwrap_err();

        let rendered = failure.to_string();
        for expected in [
            "unexpected answer 502",
            "retryable=false",
            "outcomeUnknown=true",
            "retryableWithSameKey=true",
            &format!("operationId={operation_id}"),
        ] {
            assert!(rendered.contains(expected), "{expected}: {rendered}");
        }
    }

    #[tokio::test]
    async fn rate_limited_whitelist_read_shows_the_wait() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET)
                .path(route(Tier::Read, Operation::WalletWhitelist));
            then.status(429).json_body(json!({
                "code": "backpressure",
                "retryable": true,
                "retryableWithSameKey": false,
                "retryAfterSecs": 12,
                "requestId": Uuid::new_v4(),
                "message": "human budget spent"
            }));
        });

        let failure = run(&server, Command::Wallet(WalletCommand::WhitelistList))
            .await
            .unwrap_err();

        let rendered = failure.to_string();
        assert!(rendered.contains("429"), "{rendered}");
        assert!(rendered.contains("retryAfterSecs=12"), "{rendered}");
        assert!(!failure.is_access_denied());
    }

    /// IAP's 401 or 403, which is not a gateway error body, its sign in
    /// redirect, and the gateway's own `401 Unauthenticated` refuse the
    /// operator, on the typed path and on the whitelist path alike.
    #[tokio::test]
    async fn iap_refusals_and_the_gateways_identity_refusal_are_access_denied() {
        let unauthenticated = json!({
            "code": "unauthenticated",
            "retryable": false,
            "retryableWithSameKey": false,
            "requestId": Uuid::new_v4(),
            "message": "credential rejected"
        })
        .to_string();
        let commands: [(fn() -> Command, Operation); 2] = [
            (
                || Command::Broker(BrokerCommand::Account),
                Operation::AccountFunds,
            ),
            (
                || Command::Wallet(WalletCommand::WhitelistList),
                Operation::WalletWhitelist,
            ),
        ];
        for (command, operation) in commands {
            for (status, body) in [
                (401, unauthenticated.as_str()),
                (401, "Invalid IAP credentials"),
                (403, "You don't have access"),
                (302, ""),
            ] {
                let server = MockServer::start();
                server.mock(|when, then| {
                    when.method(GET).path(route(Tier::Read, operation));
                    then.status(status)
                        .header("location", "https://accounts.google.com/o/oauth2/v2/auth")
                        .body(body);
                });

                let failure = run(&server, command()).await.unwrap_err();

                assert!(
                    failure.is_access_denied(),
                    "{operation} {status}: {failure}"
                );
            }
        }
    }

    /// The gateway's 403s refuse the operation, not the operator: a
    /// destination outside the deployment's pinned lists (`forbidden` with
    /// `destination_not_allowed`), and an operation the deployment switched
    /// off (`capability_disabled`), on the typed path and on the whitelist
    /// path. Each is the operation failing, keyed, and not an unknown outcome.
    #[tokio::test]
    async fn gateway_403_refusals_fail_the_operation_without_denying_access() {
        let refusal = |code: &str, reason: Option<&str>, message: &str| {
            json!({
                "code": code,
                "outcome": "not_applied",
                "retryable": false,
                "retryableWithSameKey": false,
                "reason": reason,
                "requestId": Uuid::new_v4(),
                "message": message
            })
        };
        let operation_id = Uuid::new_v4();
        let cases = [
            (
                Command::Wallet(WalletCommand::Withdraw {
                    amount: parse_positive_usdc("5").unwrap(),
                    asset: "USDC".to_owned(),
                    address: Address::repeat_byte(0x33),
                    operation_id: Some(operation_id),
                    reason: "treasury transfer".to_owned(),
                }),
                Operation::WalletWithdraw,
                refusal(
                    "forbidden",
                    Some("destination_not_allowed"),
                    "the address is reserved for bot withdrawals",
                ),
                "403 Forbidden",
            ),
            (
                Command::Wallet(WalletCommand::WhitelistPatchTravelRule {
                    operation_id: Some(operation_id),
                    reason: "refresh beneficiary details".to_owned(),
                }),
                Operation::WalletWhitelistPatchTravelRule,
                refusal(
                    "capability_disabled",
                    None,
                    "no Travel Rule beneficiary is configured on this deployment",
                ),
                "403 CapabilityDisabled",
            ),
        ];
        for (command, operation, body, expected) in cases {
            let server = MockServer::start();
            server.mock(|when, then| {
                when.method(POST).path(route(Tier::Write, operation));
                then.status(403).json_body(body);
            });

            let failure = run(&server, command).await.unwrap_err();

            let rendered = failure.to_string();
            for expected in [
                expected,
                "outcomeUnknown=false",
                &format!("operationId={operation_id}"),
            ] {
                assert!(rendered.contains(expected), "{expected}: {rendered}");
            }
            assert!(!failure.is_access_denied(), "{rendered}");
        }
    }

    /// Alpaca's own 403, such as a business rejection of an order, is the
    /// order failing, not the operator's access.
    #[tokio::test]
    async fn relayed_alpaca_403_is_not_access_denied() {
        let server = MockServer::start();
        preflight(&server, "Regular", Some(true));
        server.mock(|when, then| {
            when.method(POST)
                .path(route(Tier::Write, Operation::OrdersPlaceMarket));
            then.status(422).json_body(json!({
                "code": "rejected",
                "outcome": "not_applied",
                "retryable": false,
                "retryableWithSameKey": false,
                "reason": "alpaca_api",
                "alpacaStatus": 403,
                "requestId": Uuid::new_v4(),
                "message": "forbidden: insufficient buying power"
            }));
        });
        let buy = parsed(&[
            "buy",
            "--symbol",
            "COIN",
            "--quantity",
            "1",
            "--reason",
            "operator purchase",
        ])
        .unwrap();

        let failure = run(&server, buy).await.unwrap_err();

        let rendered = failure.to_string();
        assert!(rendered.contains("403"), "{rendered}");
        assert!(rendered.contains("clientOrderId=cli-"), "{rendered}");
        assert!(!failure.is_access_denied(), "{rendered}");
    }

    /// A cancel announces its broker order ID, its same key retry rule
    /// included, before the request leaves, and an unknown outcome carries
    /// that key.
    #[tokio::test]
    async fn cancel_announces_its_order_id_before_sending_and_keys_its_failure() {
        let server = MockServer::start();
        let order_id = Uuid::new_v4();
        let request = server.mock(|when, then| {
            when.method(POST)
                .path(format!("{}/orders/{order_id}/cancel", Tier::Write.prefix()));
            then.status(504).json_body(json!({
                "code": "outcome_unknown",
                "outcome": "unknown",
                "retryable": false,
                "retryableWithSameKey": true,
                "requestId": Uuid::new_v4(),
                "message": "no answer from Alpaca within the deadline"
            }));
        });
        let mut notice = Notice {
            mock: &request,
            written: Vec::new(),
            calls_at_write: Vec::new(),
        };

        let failure = execute(
            &Url::parse(&server.base_url()).unwrap(),
            "operator-token".to_owned(),
            Command::Broker(BrokerCommand::Cancel {
                order_id,
                reason: "stale order".to_owned(),
            }),
            &mut notice,
        )
        .await
        .unwrap_err();

        request.assert();
        assert_eq!(
            String::from_utf8(notice.written).unwrap(),
            format!("orderId={order_id}; retryableWithSameKey=true\n")
        );
        assert!(
            notice.calls_at_write.iter().all(|calls| *calls == 0),
            "the key reached stderr after the request: {:?}",
            notice.calls_at_write
        );
        let rendered = failure.to_string();
        for expected in [
            "outcomeUnknown=true",
            "retryableWithSameKey=true",
            &format!("orderId={order_id}"),
        ] {
            assert!(rendered.contains(expected), "{expected}: {rendered}");
        }
    }

    /// The overnight session takes only limit orders flagged extended hours,
    /// so a market order, and a limit order without `--extended-hours`, are
    /// refused before any key is announced or order sent.
    #[tokio::test]
    async fn overnight_session_refuses_market_orders_and_limit_orders_without_extended_hours() {
        let market = ["buy", "--symbol", "COIN", "--quantity", "1"];
        let limit = [
            "sell",
            "--symbol",
            "COIN",
            "--quantity",
            "1",
            "--limit-price",
            "250",
        ];
        for (arguments, operation) in [
            (market.as_slice(), Operation::OrdersPlaceMarket),
            (limit.as_slice(), Operation::OrdersPlaceExactLimit),
        ] {
            let server = MockServer::start();
            preflight(&server, "Overnight", Some(true));
            let placement = server.mock(|when, then| {
                when.method(POST).path(route(Tier::Write, operation));
                then.status(500);
            });
            let command = parsed(&[arguments, &["--reason", "operator order"]].concat()).unwrap();
            let mut notice = Vec::new();

            let failure = execute(
                &Url::parse(&server.base_url()).unwrap(),
                "operator-token".to_owned(),
                command,
                &mut notice,
            )
            .await
            .unwrap_err();

            match operation {
                Operation::OrdersPlaceMarket => {
                    assert!(
                        matches!(failure, Failure::OvernightMarketOrder),
                        "{failure:?}"
                    );
                }
                _ => assert!(
                    matches!(failure, Failure::OvernightWithoutExtendedHours),
                    "{failure:?}"
                ),
            }
            assert_eq!(placement.calls(), 0);
            assert!(notice.is_empty(), "{}", String::from_utf8_lossy(&notice));
        }
    }

    /// `st0x-alpaca` sizes a market order on whole shares unless the asset
    /// is fractionable, missing metadata included, so a fractional market
    /// order on such an asset is refused rather than floored.
    #[tokio::test]
    async fn fractional_market_order_on_an_asset_not_known_fractionable_is_refused() {
        for fractionable in [Some(false), None] {
            let server = MockServer::start();
            preflight(&server, "Regular", fractionable);
            let placement = server.mock(|when, then| {
                when.method(POST)
                    .path(route(Tier::Write, Operation::OrdersPlaceMarket));
                then.status(500);
            });
            let buy = parsed(&[
                "buy",
                "--symbol",
                "COIN",
                "--quantity",
                "1.5",
                "--reason",
                "operator purchase",
            ])
            .unwrap();

            let failure = run(&server, buy).await.unwrap_err();

            assert!(
                matches!(failure, Failure::FractionalMarketOrder { .. }),
                "{fractionable:?}: {failure:?}"
            );
            assert_eq!(placement.calls(), 0);
        }
    }

    /// Orders the session and the share grid allow are sent: a whole market
    /// order on an asset that is not fractionable, a fractional market order
    /// on a fractionable asset, and an overnight exact limit order flagged
    /// extended hours, whose quantity the library never sizes.
    #[tokio::test]
    async fn orders_the_session_and_share_grid_allow_are_sent() {
        let cases: [(&str, Option<bool>, &[&str], Operation); 3] = [
            (
                "Regular",
                Some(false),
                &["buy", "--symbol", "COIN", "--quantity", "2"],
                Operation::OrdersPlaceMarket,
            ),
            (
                "Regular",
                Some(true),
                &["sell", "--symbol", "COIN", "--quantity", "1.5"],
                Operation::OrdersPlaceMarket,
            ),
            (
                "Overnight",
                Some(false),
                &[
                    "buy",
                    "--symbol",
                    "COIN",
                    "--quantity",
                    "1.5",
                    "--limit-price",
                    "250",
                    "--extended-hours",
                ],
                Operation::OrdersPlaceExactLimit,
            ),
        ];
        for (session, fractionable, arguments, operation) in cases {
            let server = MockServer::start();
            preflight(&server, session, fractionable);
            let placement = server.mock(|when, then| {
                when.method(POST).path(route(Tier::Write, operation));
                then.status(200).json_body(json!({
                    "orderId": "order-1",
                    "symbol": "COIN",
                    "shares": "1.5",
                    "direction": "buy",
                    "placedAt": "2026-10-12T14:00:00Z",
                    "extendedHours": false,
                    "limitPrice": null
                }));
            });
            let command = parsed(&[arguments, &["--reason", "operator order"]].concat()).unwrap();

            let output = run(&server, command).await.unwrap();

            placement.assert();
            assert_eq!(output["order"]["orderId"], "order-1", "{arguments:?}");
        }
    }

    /// `convert` waits until the conversion settles, as the legacy
    /// `alpaca-convert` does, and prints the settled order rather than the
    /// accepted one.
    #[tokio::test]
    async fn convert_waits_for_the_conversion_to_settle() {
        let server = MockServer::start();
        let order_id = Uuid::new_v4();
        let order = |status: &str, filled: Value| {
            json!({
                "id": order_id,
                "symbol": "USDCUSD",
                "qty": "10",
                "status": status,
                "filled_qty": filled,
                "created_at": "2026-10-12T14:00:00Z"
            })
        };
        let submit = server.mock(|when, then| {
            when.method(POST)
                .path(route(Tier::Write, Operation::ConversionsSubmit));
            then.status(200).json_body(order("new", json!("0")));
        });
        let settled = server.mock(|when, then| {
            when.method(GET)
                .path(format!("{}/conversions/{order_id}", Tier::Write.prefix()));
            then.status(200).json_body(order("filled", json!("10")));
        });
        let convert = parsed(&[
            "convert",
            "--direction",
            "to-usd",
            "--amount",
            "10",
            "--reason",
            "fund purchase",
        ])
        .unwrap();

        let output = run(&server, convert).await.unwrap();

        submit.assert();
        settled.assert();
        assert_eq!(output["order"]["status"], "filled");
    }
}
