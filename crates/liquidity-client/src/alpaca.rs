//! Commands shared by the T0 and S01 operator entry points for the account bound
//! Alpaca gateways.

use std::error::Error as StdError;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use st0x_alpaca::GatewayHopError;
use st0x_alpaca::broker::{
    AccountActivitiesQuery, AlpacaBrokerApiError as BrokerError, AlpacaLimitOrder,
    AlpacaLimitPrice, ClientOrderId, ConversionOrder, ConversionOrders, Direction, MarketOrder,
};
use st0x_alpaca::core::Network as TokenizationNetwork;
use st0x_alpaca::st0x_finance::{FractionalShares, Positive, Symbol, Usd, Usdc};
use st0x_alpaca::tokenization::{
    AlpacaTokenizationError as TokenizationError, IssuerRequestId, TokenizationLookups,
    TokenizationRequestId,
};
use st0x_alpaca::wallet::{
    AlpacaTransferId, AlpacaWalletError as WalletError, Network as WalletNetwork, TokenSymbol,
    WalletTransfers,
};
use st0x_alpaca_gateway_api::client::{GatewayClient, StaticToken};
use st0x_alpaca_gateway_api::dto::wallet::{
    TravelRulePatchRequest, WhitelistCreateRequest, WhitelistRemoveRequest, WhitelistResponse,
};
use st0x_alpaca_gateway_api::{ErrorBody, Method, Operation, Tier};
use url::Url;
use uuid::Uuid;

const CLIENT_DEADLINE_MARGIN: Duration = Duration::from_secs(15);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Operations available through an account bound Alpaca gateway.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
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
    /// Latest regular or overnight quote for one symbol.
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
    /// Submit one USD and USDC conversion order.
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
    #[arg(long, value_parser = parse_positive_shares)]
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
    #[arg(long, value_parser = parse_positive_decimal)]
    amount: PositiveDecimal,
    #[arg(long)]
    client_order_id: Option<Uuid>,
    #[arg(long, value_parser = nonblank_reason)]
    reason: String,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ConversionDirection {
    ToUsd,
    ToUsdc,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum QuoteFeed {
    Latest,
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

#[derive(Clone, Debug)]
struct PositiveDecimal(String);

fn nonblank_reason(value: &str) -> Result<String, String> {
    if value.trim().is_empty() {
        Err("must not be blank".to_owned())
    } else {
        Ok(value.to_owned())
    }
}

fn parse_positive_shares(value: &str) -> Result<Positive<FractionalShares>, String> {
    let amount = value
        .parse::<FractionalShares>()
        .map_err(|error| error.to_string())?;
    Positive::new(amount).map_err(|error| error.to_string())
}

fn parse_positive_usdc(value: &str) -> Result<Positive<Usdc>, String> {
    let amount = value.parse::<Usdc>().map_err(|error| error.to_string())?;
    Positive::new(amount).map_err(|error| error.to_string())
}

fn parse_positive_decimal(value: &str) -> Result<PositiveDecimal, String> {
    parse_positive_usdc(value)?;
    Ok(PositiveDecimal(value.to_owned()))
}

impl Command {
    #[must_use]
    pub(crate) const fn tier(&self) -> Tier {
        match self {
            Self::Buy(_)
            | Self::Sell(_)
            | Self::Cancel { .. }
            | Self::Withdraw { .. }
            | Self::Whitelist { .. }
            | Self::WhitelistPatchTravelRule { .. }
            | Self::Unwhitelist { .. }
            | Self::Convert(_)
            | Self::Journal { .. }
            | Self::Tokenize { .. } => Tier::Write,
            Self::Account
            | Self::Cash
            | Self::Positions
            | Self::PositionMark { .. }
            | Self::Activities { .. }
            | Self::MarketStatus
            | Self::LatestTrade { .. }
            | Self::Quote { .. }
            | Self::Asset { .. }
            | Self::OrderStatus { .. }
            | Self::OrderFind { .. }
            | Self::Deposit { .. }
            | Self::WhitelistList
            | Self::Transfers { .. }
            | Self::Transfer { .. }
            | Self::FindDeposit { .. }
            | Self::ConversionStatus { .. }
            | Self::ConversionFind { .. }
            | Self::TokenizationRequest { .. }
            | Self::MintStatus { .. }
            | Self::Redeem { .. }
            | Self::TokenizationRequests { .. } => Tier::Read,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Failure {
    #[error("{0}")]
    Gateway(String),
    #[error("could not build the Alpaca gateway client: {0}")]
    Client(String),
    #[error("could not encode the Alpaca gateway response as JSON: {0}")]
    Json(#[from] serde_json::Error),
}

impl Failure {
    #[must_use]
    pub(crate) fn is_access_denied(&self) -> bool {
        let message = self.to_string().to_ascii_lowercase();
        message.contains(" 401")
            || message.contains("401 ")
            || message.contains("unauthenticated")
            || message.contains(" 403")
            || message.contains("403 ")
            || message.contains("forbidden")
    }

    fn from_source(error: &(dyn StdError + 'static)) -> Self {
        Self::Gateway(render_error(error))
    }

    fn with_key(
        error: &(dyn StdError + 'static),
        key: &str,
        value: impl std::fmt::Display,
    ) -> Self {
        Self::Gateway(format!("{}; {key}={value}", render_error(error)))
    }
}

/// Sends one operator command and returns the compact JSON value the entry point prints.
#[allow(clippy::too_many_lines)] // Keeping command dispatch in one exhaustive match avoids divergent routing tables.
pub(crate) async fn execute(base: &Url, token: String, command: Command) -> Result<Value, Failure> {
    let tier = command.tier();
    let client = GatewayClient::new(base.as_str(), tier, StaticToken(token.clone()))
        .map_err(|error| Failure::Client(error.to_string()))?;

    match command {
        Command::Account => value(
            client
                .broker()
                .account_funds()
                .await
                .map_err(|error| Failure::from_source(&error))?,
        ),
        Command::Cash => {
            let withdrawable = client
                .broker()
                .withdrawable_cash_cents()
                .await
                .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({ "withdrawableCashCents": withdrawable }))
        }
        Command::Positions => value(
            client
                .broker()
                .fetch_inventory()
                .await
                .map_err(|error| Failure::from_source(&error))?,
        ),
        Command::PositionMark { symbol } => {
            let mark = client
                .broker()
                .fetch_position_mark(&symbol)
                .await
                .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({ "symbol": symbol, "mark": mark }))
        }
        Command::Activities {
            types,
            after,
            until,
        } => {
            let activities = client
                .broker()
                .fetch_account_activities(&AccountActivitiesQuery {
                    activity_types: types,
                    after,
                    until,
                })
                .await
                .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({ "activities": activities }))
        }
        Command::MarketStatus => value(
            client
                .broker()
                .market_session_status()
                .await
                .map_err(|error| Failure::from_source(&error))?,
        ),
        Command::LatestTrade { symbol } => {
            let price = client
                .broker()
                .fetch_latest_trade_price(&symbol)
                .await
                .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({ "symbol": symbol, "price": price }))
        }
        Command::Quote { symbol, feed } => quote(&client, symbol, feed).await,
        Command::Asset { symbol } => value(
            client
                .broker()
                .get_asset_details(&symbol)
                .await
                .map_err(|error| Failure::from_source(&error))?,
        ),
        Command::Buy(args) => place_order(&client, args, Direction::Buy).await,
        Command::Sell(args) => place_order(&client, args, Direction::Sell).await,
        Command::Cancel { order_id, reason } => {
            let outcome = client
                .broker()
                .cancel_order(&order_id.to_string(), Some(&reason))
                .await
                .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({ "orderId": order_id, "outcome": outcome }))
        }
        Command::OrderStatus { order_id } => value(
            client
                .broker()
                .get_order_status(&order_id.to_string())
                .await
                .map_err(|error| Failure::from_source(&error))?,
        ),
        Command::OrderFind { client_order_id } => {
            let key = ClientOrderId::cli(client_order_id);
            let order = client
                .broker()
                .get_order_by_client_order_id(&key)
                .await
                .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({ "clientOrderId": key, "order": order }))
        }
        Command::Deposit { asset, network } => {
            let address = client
                .wallet()
                .get_wallet_address(
                    &TokenSymbol::new(asset.clone()),
                    &WalletNetwork::new(network.clone()),
                )
                .await
                .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({ "asset": asset, "network": network, "address": address }))
        }
        Command::Withdraw {
            amount,
            asset,
            address,
            operation_id,
            reason,
        } => {
            let operation_id = operation_id.unwrap_or_else(Uuid::new_v4);
            let transfer = client
                .wallet()
                .initiate_withdrawal(
                    amount,
                    &TokenSymbol::new(asset),
                    &address,
                    operation_id,
                    Some(&reason),
                )
                .await
                .map_err(|error| Failure::with_key(&error, "operationId", operation_id))?;
            Ok(json!({ "operationId": operation_id, "transfer": transfer }))
        }
        Command::Whitelist {
            address,
            asset,
            operation_id,
            reason,
        } => {
            let operation_id = operation_id.unwrap_or_else(Uuid::new_v4);
            let request = WhitelistCreateRequest {
                address,
                asset: TokenSymbol::new(asset),
                operation_id,
                reason: Some(reason),
            };
            let answer: WhitelistResponse = RawGateway::new(base.clone(), tier, token.clone())?
                .send(Operation::WalletWhitelistCreate, &[], Some(&request))
                .await?;
            Ok(json!({ "operationId": operation_id, "entries": answer.entries }))
        }
        Command::WhitelistList => {
            let answer: WhitelistResponse = RawGateway::new(base.clone(), tier, token.clone())?
                .send::<(), _>(Operation::WalletWhitelist, &[], None)
                .await?;
            Ok(json!({ "entries": answer.entries }))
        }
        Command::WhitelistPatchTravelRule {
            operation_id,
            reason,
        } => {
            let operation_id = operation_id.unwrap_or_else(Uuid::new_v4);
            let request = TravelRulePatchRequest {
                operation_id,
                reason: Some(reason),
            };
            let answer: WhitelistResponse = RawGateway::new(base.clone(), tier, token.clone())?
                .send(
                    Operation::WalletWhitelistPatchTravelRule,
                    &[],
                    Some(&request),
                )
                .await?;
            Ok(json!({ "operationId": operation_id, "entries": answer.entries }))
        }
        Command::Unwhitelist {
            address,
            operation_id,
            reason,
        } => {
            let operation_id = operation_id.unwrap_or_else(Uuid::new_v4);
            let request = WhitelistRemoveRequest {
                operation_id,
                reason: Some(reason),
            };
            let answer: WhitelistResponse = RawGateway::new(base.clone(), tier, token.clone())?
                .send(
                    Operation::WalletWhitelistRemove,
                    &[("address", address.to_string())],
                    Some(&request),
                )
                .await?;
            Ok(json!({ "operationId": operation_id, "entries": answer.entries }))
        }
        Command::Transfers { pending } => {
            let mut transfers = client
                .wallet()
                .list_all_transfers()
                .await
                .map_err(|error| Failure::from_source(&error))?;
            if pending {
                transfers.retain(|transfer| transfer.status.is_pending());
            }
            Ok(json!({ "transfers": transfers }))
        }
        Command::Transfer { transfer_id } => {
            let (transfer, reported_fees) = client
                .wallet()
                .get_transfer_with_fees(&AlpacaTransferId::from(transfer_id))
                .await
                .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({ "transfer": transfer, "reportedFees": reported_fees }))
        }
        Command::FindDeposit { tx_hash } => {
            let deposit = client
                .wallet()
                .find_deposit_by_tx_hash(&tx_hash)
                .await
                .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({ "deposit": deposit }))
        }
        Command::Convert(args) => convert(&client, args).await,
        Command::ConversionStatus { order_id } => value(
            client
                .broker()
                .get_conversion_order(order_id)
                .await
                .map_err(|error| Failure::from_source(&error))?,
        ),
        Command::ConversionFind { client_order_id } => {
            let key = ClientOrderId::cli(client_order_id);
            let order = client
                .broker()
                .find_conversion_order(&key)
                .await
                .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({ "clientOrderId": key, "order": order }))
        }
        Command::Journal {
            counterparty,
            symbol,
            quantity,
            operation_id,
            reason,
        } => {
            let operation_id = operation_id.unwrap_or_else(Uuid::new_v4);
            let journal = client
                .broker()
                .create_journal(counterparty, &symbol, quantity, operation_id, reason)
                .await
                .map_err(|error| Failure::with_key(&error, "operationId", operation_id))?;
            Ok(json!({ "operationId": operation_id, "journal": journal }))
        }
        Command::Tokenize {
            symbol,
            quantity,
            recipient,
            network,
            issuer_request_id,
            reason,
        } => {
            let issuer_request_id = IssuerRequestId(issuer_request_id.unwrap_or_else(Uuid::new_v4));
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
                .map_err(|error| {
                    Failure::with_key(&error, "issuerRequestId", &issuer_request_id)
                })?;
            Ok(json!({ "issuerRequestId": issuer_request_id, "request": request }))
        }
        Command::TokenizationRequest {
            request_id,
            network,
        } => value(
            client
                .tokenization(network.into())
                .get_request(&request_id)
                .await
                .map_err(|error| Failure::from_source(&error))?,
        ),
        Command::MintStatus {
            issuer_request_id,
            network,
        } => {
            let request = client
                .tokenization(network.into())
                .find_mint_by_issuer_request_id(&issuer_request_id)
                .await
                .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({ "issuerRequestId": issuer_request_id, "request": request }))
        }
        Command::Redeem { tx_hash, network } => {
            let request = client
                .tokenization(network.into())
                .find_redemption_by_tx(&tx_hash)
                .await
                .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({ "transactionHash": tx_hash, "request": request }))
        }
        Command::TokenizationRequests { network, pending } => {
            let tokenization = client.tokenization(network.into());
            let requests = if pending {
                tokenization.list_pending_requests().await
            } else {
                tokenization.list_requests().await
            }
            .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({ "requests": requests }))
        }
    }
}

async fn quote(
    client: &GatewayClient<StaticToken>,
    symbol: Symbol,
    feed: QuoteFeed,
) -> Result<Value, Failure> {
    match feed {
        QuoteFeed::Latest => {
            let quote = client
                .broker()
                .fetch_latest_quote(&symbol)
                .await
                .map_err(|error| Failure::from_source(&error))?;
            Ok(json!({
                "symbol": symbol,
                "feed": "latest",
                "bid": quote.bid(),
                "ask": quote.ask(),
            }))
        }
        QuoteFeed::Overnight => {
            let quote = client
                .broker()
                .fetch_latest_overnight_quote(&symbol)
                .await
                .map_err(|error| Failure::from_source(&error))?;
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

async fn place_order(
    client: &GatewayClient<StaticToken>,
    args: OrderArgs,
    direction: Direction,
) -> Result<Value, Failure> {
    let OrderArgs {
        symbol,
        quantity,
        limit_price,
        extended_hours,
        client_order_id,
        reason,
    } = args;
    let key = ClientOrderId::cli(client_order_id.unwrap_or_else(Uuid::new_v4));
    let order = match limit_price {
        Some(limit_price) => {
            client
                .broker()
                .place_alpaca_limit_order(
                    AlpacaLimitOrder {
                        symbol,
                        shares: quantity,
                        direction,
                        limit_price,
                        extended_hours,
                        client_order_id: key.clone(),
                    },
                    reason,
                )
                .await
        }
        None => {
            client
                .broker()
                .place_market_order(
                    MarketOrder {
                        symbol,
                        shares: quantity,
                        direction,
                        client_order_id: key.clone(),
                    },
                    Some(&reason),
                )
                .await
        }
    }
    .map_err(|error| Failure::with_key(&error, "clientOrderId", &key))?;
    Ok(json!({ "clientOrderId": key, "order": order }))
}

async fn convert(client: &GatewayClient<StaticToken>, args: ConvertArgs) -> Result<Value, Failure> {
    let ConvertArgs {
        direction,
        amount,
        client_order_id,
        reason,
    } = args;
    let conversion = match direction {
        ConversionDirection::ToUsd => {
            ConversionOrder::SellUsdc(parse_positive_usdc(&amount.0).map_err(Failure::Client)?)
        }
        ConversionDirection::ToUsdc => {
            let amount = amount
                .0
                .parse::<Usd>()
                .map_err(|error| Failure::Client(error.to_string()))?;
            ConversionOrder::BuyWithUsd(
                Positive::new(amount).map_err(|error| Failure::Client(error.to_string()))?,
            )
        }
    };
    let key = ClientOrderId::cli(client_order_id.unwrap_or_else(Uuid::new_v4));
    let order = client
        .broker()
        .submit_conversion(conversion, &key, Some(&reason))
        .await
        .map_err(|error| Failure::with_key(&error, "clientOrderId", &key))?;
    Ok(json!({ "clientOrderId": key, "order": order }))
}

fn value(value: impl Serialize) -> Result<Value, Failure> {
    serde_json::to_value(value).map_err(Failure::from)
}

fn render_error(error: &(dyn StdError + 'static)) -> String {
    let mut messages = Vec::new();
    let mut hop = None;
    let mut current = Some(error);
    while let Some(source) = current {
        let message = source.to_string();
        if messages.last() != Some(&message) {
            messages.push(message);
        }
        hop = hop.or_else(|| gateway_hop(source));
        current = source.source();
    }
    let mut rendered = messages.join(": ");
    if let Some(hop) = hop {
        use std::fmt::Write as _;
        let _ = write!(
            rendered,
            "; retryable={}; outcomeUnknown={}; retryableWithSameKey={}",
            hop.retryable, hop.outcome_unknown, hop.retryable_with_same_key
        );
        if let Some(wait) = hop.retry_after {
            let _ = write!(rendered, "; retryAfterSecs={}", wait.as_secs());
        }
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

struct RawGateway {
    base: Url,
    tier: Tier,
    token: String,
    http: reqwest::Client,
}

impl RawGateway {
    fn new(base: Url, tier: Tier, token: String) -> Result<Self, Failure> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|error| Failure::Client(error.to_string()))?;
        Ok(Self {
            base,
            tier,
            token,
            http,
        })
    }

    async fn send<Request, Response>(
        &self,
        operation: Operation,
        params: &[(&'static str, String)],
        body: Option<&Request>,
    ) -> Result<Response, Failure>
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
        let bound = operation.deadline() + CLIENT_DEADLINE_MARGIN;
        let response = tokio::time::timeout(bound, request.send())
            .await
            .map_err(|_| no_answer(operation, format!("no answer within {bound:?}")))?
            .map_err(|error| no_answer(operation, error.to_string()))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| no_answer(operation, error.to_string()))?;

        if status.is_success() {
            return serde_json::from_slice(&bytes).map_err(|error| {
                Failure::Gateway(format!(
                    "unexpected gateway answer {}: {error}; outcomeUnknown={}",
                    status.as_u16(),
                    operation.mutates()
                ))
            });
        }

        serde_json::from_slice::<ErrorBody>(&bytes).map_or_else(
            |_| {
                Err(Failure::Gateway(format!(
                    "unexpected gateway answer {}: {}; outcomeUnknown={}",
                    status.as_u16(),
                    String::from_utf8_lossy(&bytes)
                        .chars()
                        .take(512)
                        .collect::<String>(),
                    operation.mutates()
                )))
            },
            |body| {
                Err(Failure::Gateway(render_gateway_error(
                    status.as_u16(),
                    &body,
                )))
            },
        )
    }

    fn url(&self, operation: Operation, params: &[(&'static str, String)]) -> Result<Url, Failure> {
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
                Some(name) => params
                    .iter()
                    .find(|(parameter, _)| *parameter == name)
                    .map(|(_, value)| value.as_str())
                    .ok_or_else(|| {
                        Failure::Client(format!("missing gateway path parameter {name}"))
                    })?,
                None => segment,
            };
            segments.push(value);
        }
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|()| Failure::Client("gateway base URL cannot hold path segments".to_owned()))?
            .pop_if_empty()
            .extend(segments);
        Ok(url)
    }
}

fn no_answer(operation: Operation, detail: impl std::fmt::Display) -> Failure {
    Failure::Gateway(format!(
        "no answer from the Alpaca gateway: {detail}; outcomeUnknown={}; retryableWithSameKey={}",
        operation.mutates(),
        operation.resendable_with_same_key()
    ))
}

fn render_gateway_error(status: u16, body: &ErrorBody) -> String {
    format!(
        "{status} {:?}: {}; requestId={}; outcome={:?}; retryable={}; retryableWithSameKey={}; retryAfterSecs={:?}; reason={:?}; alpacaStatus={:?}",
        body.code,
        body.message,
        body.request_id,
        body.outcome,
        body.retryable,
        body.retryable_with_same_key,
        body.retry_after_secs,
        body.reason,
        body.alpaca_status
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::Parser;
    use httpmock::prelude::*;
    use st0x_alpaca::broker::AlpacaBrokerApiError;

    use super::*;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        command: Command,
    }

    #[test]
    fn mutation_reason_is_required_and_nonblank() {
        let missing =
            TestCli::try_parse_from(["client", "cancel", "11111111-1111-4111-8111-111111111111"]);
        assert!(missing.is_err());

        let blank = TestCli::try_parse_from([
            "client",
            "cancel",
            "11111111-1111-4111-8111-111111111111",
            "--reason",
            " ",
        ]);
        assert!(blank.is_err());
    }

    #[test]
    fn no_command_accepts_an_account_identifier() {
        let result = TestCli::try_parse_from([
            "client",
            "account",
            "--account-id",
            "11111111-1111-4111-8111-111111111111",
        ]);
        assert!(result.is_err());
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
        let rendered = Failure::from_source(&source).to_string();
        assert!(rendered.contains("outcomeUnknown=true"), "{rendered}");
        assert!(rendered.contains("retryableWithSameKey=true"), "{rendered}");
        assert!(rendered.contains("retryAfterSecs=12"), "{rendered}");
    }

    #[tokio::test]
    async fn account_read_uses_the_read_tier_and_returns_contract_json() {
        let server = MockServer::start();
        let route = format!("{}{}", Tier::Read.prefix(), Operation::AccountFunds.path());
        let request = server.mock(|when, then| {
            when.method(GET)
                .path(route)
                .header("authorization", "Bearer operator-token");
            then.status(200).json_body(json!({
                "balance": 100,
                "buyingPower": 80,
                "withdrawable": 70
            }));
        });

        let output = execute(
            &Url::parse(&server.base_url()).unwrap(),
            "operator-token".to_owned(),
            Command::Account,
        )
        .await
        .unwrap();

        request.assert();
        assert_eq!(
            output,
            json!({ "balance": 100, "buyingPower": 80, "withdrawable": 70 })
        );
    }

    #[tokio::test]
    async fn whitelist_patch_uses_the_write_tier_and_echoes_its_operation_id() {
        let server = MockServer::start();
        let operation_id = Uuid::new_v4();
        let route = format!(
            "{}{}",
            Tier::Write.prefix(),
            Operation::WalletWhitelistPatchTravelRule.path()
        );
        let request = server.mock(|when, then| {
            when.method(POST)
                .path(route)
                .header("authorization", "Bearer operator-token")
                .json_body(json!({
                    "operationId": operation_id,
                    "reason": "Apply the configured beneficiary"
                }));
            then.status(200).json_body(json!({ "entries": [] }));
        });

        let output = execute(
            &Url::parse(&server.base_url()).unwrap(),
            "operator-token".to_owned(),
            Command::WhitelistPatchTravelRule {
                operation_id: Some(operation_id),
                reason: "Apply the configured beneficiary".to_owned(),
            },
        )
        .await
        .unwrap();

        request.assert();
        assert_eq!(
            output,
            json!({ "operationId": operation_id, "entries": [] })
        );
    }
}
