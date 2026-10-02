//! Live pricing-service subscription and dashboard price read model.

use alloy::primitives::{Address, B256};
use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use rain_math_float::{Float, FloatError};
use rand::Rng;
use st0x_pricing_types::{
    ClientFrame, ErrorFrame, PongFrame, PriceFrame, ServerFrame, SubscribeFrame, Venue, WireFloat,
};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::sync::{RwLock, broadcast};
use tokio::time::{Interval, MissedTickBehavior, interval, sleep, timeout};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{AUTHORIZATION, HeaderValue};
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message};
use tracing::{debug, info, warn};

use st0x_config::{ChainAssets, PricingAuth, PricingCtx};
use st0x_dto::{EquityPrice, EquityPriceStatus, Statement};
use st0x_evm::{Chain, SettlementStable};
use st0x_finance::Symbol;
use st0x_float_macro::float;

use crate::position::PriceObservation;

// The pricing service's existing `oracle` identity is scoped to Raindex quotes.
const CONSUMER: &str = "oracle";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const RECONNECT_MIN_DELAY: Duration = Duration::from_secs(5);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(60);
const RECONNECT_MAX_JITTER_MS: u64 = 1_000;
const EXPIRY_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Default)]
struct ReconnectBackoff {
    consecutive_failures: u32,
}

impl ReconnectBackoff {
    fn record_failure(&mut self) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
    }

    fn reset(&mut self) {
        self.consecutive_failures = 0;
    }

    fn delay(&self, jitter: Duration) -> Duration {
        let exponent = self.consecutive_failures.saturating_sub(1).min(4);
        let jitter = jitter.min(Duration::from_millis(RECONNECT_MAX_JITTER_MS));
        let max_base =
            RECONNECT_MAX_DELAY.saturating_sub(Duration::from_millis(RECONNECT_MAX_JITTER_MS));
        let base = RECONNECT_MIN_DELAY
            .saturating_mul(2_u32.saturating_pow(exponent))
            .min(max_base);
        base.saturating_add(jitter)
    }
}

fn reconnect_jitter() -> Duration {
    Duration::from_millis(rand::thread_rng().gen_range(0..=RECONNECT_MAX_JITTER_MS))
}

#[derive(Clone, Debug)]
struct ExpectedPrice {
    symbol: Symbol,
    /// Chains this bot trades this symbol on, each mapped to that chain's
    /// tokenized-equity-derivative address (the `base` token a valid quote
    /// must name). A frame whose `chain_id` is absent here is for a venue we
    /// price but do not trade -- discarded, never applied to the read model.
    traded: HashMap<u64, Address>,
}

#[derive(Clone, Debug)]
struct AvailablePrice {
    price_usd: Float,
    /// Mid price of one underlying share, `None` when the frame does not carry
    /// the underlying rates.
    underlying_price_usd: Option<Float>,
    observed_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

/// Told when a symbol gains a usable mark, so a check that declined for want
/// of a price runs again. Nothing else wakes it while balances are unchanged.
#[async_trait::async_trait]
pub(crate) trait MarkListener: Send + Sync {
    async fn mark_available(&self, symbol: &Symbol);
}

/// Process-local latest-price view: dashboard projections read every symbol,
/// and the equity rebalancer values a never-filled symbol with [`Self::mark`].
#[derive(Clone)]
pub(crate) struct EquityPriceStore {
    prices: Arc<RwLock<HashMap<Symbol, Option<AvailablePrice>>>>,
    mark_listener: Arc<std::sync::OnceLock<Arc<dyn MarkListener>>>,
}

impl std::fmt::Debug for EquityPriceStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EquityPriceStore")
            .field("prices", &self.prices)
            .finish_non_exhaustive()
    }
}

impl EquityPriceStore {
    /// Seeds one `None` entry per symbol the bot trades on any chain, so the
    /// snapshot reports every tradable symbol as unavailable until a price
    /// arrives. Spans the same chains as [`EquityPriceMonitor::new`] so a
    /// symbol the monitor accepts is never dropped by the store.
    pub(crate) fn new<'a>(traded_chains: impl IntoIterator<Item = &'a ChainAssets>) -> Self {
        let prices = traded_chains
            .into_iter()
            .flat_map(|assets| assets.equities.symbols.keys().cloned())
            .map(|symbol| (symbol, None))
            .collect();

        Self {
            prices: Arc::new(RwLock::new(prices)),
            mark_listener: Arc::default(),
        }
    }

    /// Attaches the listener told when a symbol gains a usable mark, and tells
    /// it at once about every symbol already holding one: the price monitor
    /// runs before the listener exists, and a mark that arrived first never
    /// notifies again while it stays usable. Only the first listener is kept.
    pub(crate) async fn notify_marks_to(&self, listener: Arc<dyn MarkListener>) {
        if self.mark_listener.set(listener.clone()).is_err() {
            warn!(target: "dashboard", "A mark listener is already attached; keeping the first");
            return;
        }

        // Read after attaching: a mark stored before this read is replayed
        // here, and one stored after it notifies through `update`.
        let now = Utc::now();
        let marked: Vec<Symbol> = self
            .prices
            .read()
            .await
            .iter()
            .filter(|(_, price)| {
                price.as_ref().is_some_and(|price| {
                    price.underlying_price_usd.is_some() && price.expires_at > now
                })
            })
            .map(|(symbol, _)| symbol.clone())
            .collect();
        for symbol in &marked {
            listener.mark_available(symbol).await;
        }
    }

    pub(crate) async fn snapshot(&self, now: DateTime<Utc>) -> Vec<EquityPrice> {
        let mut snapshot = {
            let mut prices = self.prices.write().await;
            let _ = take_expired(&mut prices, now);

            prices
                .iter()
                .map(|(symbol, available)| EquityPrice {
                    symbol: symbol.clone(),
                    status: available
                        .as_ref()
                        .map_or(EquityPriceStatus::Unavailable, |price| {
                            EquityPriceStatus::Available {
                                price_usd: price.price_usd,
                                observed_at: price.observed_at,
                                expires_at: price.expires_at,
                            }
                        }),
                })
                .collect::<Vec<_>>()
        };
        snapshot.sort_by(|left, right| left.symbol.cmp(&right.symbol));
        snapshot
    }

    /// A store holding one live mark for `symbol`, observed now, with a
    /// wrapper ratio of 1.
    #[cfg(test)]
    pub(crate) fn with_live_mark(symbol: Symbol, price_usd: Float) -> Self {
        let now = Utc::now();
        let price = AvailablePrice {
            price_usd,
            underlying_price_usd: Some(price_usd),
            observed_at: now,
            expires_at: now + chrono::TimeDelta::seconds(30),
        };

        Self {
            prices: Arc::new(RwLock::new(HashMap::from([(symbol, Some(price))]))),
            mark_listener: Arc::default(),
        }
    }

    /// The symbol's mark if one is live at `now`: the mid price of one
    /// underlying share in the settlement stable. `None` when the live frame
    /// does not carry the underlying rates.
    pub(crate) async fn mark(
        &self,
        symbol: &Symbol,
        now: DateTime<Utc>,
    ) -> Option<PriceObservation> {
        self.prices
            .read()
            .await
            .get(symbol)?
            .as_ref()
            .filter(|price| price.expires_at > now)
            .and_then(|price| {
                Some(PriceObservation {
                    price: price.underlying_price_usd?,
                    observed_at: price.observed_at,
                })
            })
    }

    /// Stores `price` unless an equal or newer one is held, and tells the mark
    /// listener when the symbol had no usable mark before. A held mark that
    /// has expired counts as missing, as it does for [`Self::mark`], even
    /// before the expiry sweep removes it.
    async fn update(&self, symbol: &Symbol, price: AvailablePrice) -> bool {
        let mut prices = self.prices.write().await;
        let Some(value) = prices.get_mut(symbol) else {
            warn!(target: "dashboard", %symbol, "Pricing quote for a symbol absent from the store");
            return false;
        };
        if value
            .as_ref()
            .is_some_and(|current| current.observed_at >= price.observed_at)
        {
            return false;
        }

        let now = Utc::now();
        let mark_arrives = price.underlying_price_usd.is_some()
            && value.as_ref().is_none_or(|current| {
                current.underlying_price_usd.is_none() || current.expires_at <= now
            });
        *value = Some(price);
        drop(prices);

        if mark_arrives && let Some(listener) = self.mark_listener.get() {
            listener.mark_available(symbol).await;
        }
        true
    }

    async fn make_unavailable(&self, symbol: &Symbol) -> bool {
        self.prices
            .write()
            .await
            .get_mut(symbol)
            .is_some_and(|available| available.take().is_some())
    }

    async fn make_all_unavailable(&self) -> Vec<Symbol> {
        let mut prices = self.prices.write().await;
        prices
            .iter_mut()
            .filter_map(|(symbol, available)| available.take().map(|_| symbol.clone()))
            .collect()
    }

    async fn expire(&self, now: DateTime<Utc>) -> Vec<Symbol> {
        let mut prices = self.prices.write().await;
        take_expired(&mut prices, now)
    }
}

fn take_expired(
    prices: &mut HashMap<Symbol, Option<AvailablePrice>>,
    now: DateTime<Utc>,
) -> Vec<Symbol> {
    prices
        .iter_mut()
        .filter_map(|(symbol, available)| {
            let expired = available
                .as_ref()
                .is_some_and(|price| price.expires_at <= now);
            expired.then(|| {
                *available = None;
                symbol.clone()
            })
        })
        .collect()
}

/// Resilient subscriber: pricing outages degrade the dashboard read model but
/// never terminate or pause the trading runtime.
#[derive(Clone)]
pub(crate) struct EquityPriceMonitor {
    ctx: PricingCtx,
    expected: Arc<HashMap<String, ExpectedPrice>>,
    store: EquityPriceStore,
    sender: broadcast::Sender<Statement>,
}

impl EquityPriceMonitor {
    /// `traded_chains` yields, for every chain this bot trades on, that
    /// chain's id and equity assets. A symbol's price is only accepted on a
    /// chain it appears under here: the pricing service publishes the same
    /// symbol on chains we merely price but never trade (e.g. Robinhood), and
    /// those frames must be discarded rather than clobber the tradable
    /// chain's price.
    pub(crate) fn new<'a>(
        ctx: PricingCtx,
        traded_chains: impl IntoIterator<Item = (u64, &'a ChainAssets)>,
        store: EquityPriceStore,
        sender: broadcast::Sender<Statement>,
    ) -> Self {
        let mut expected: HashMap<String, ExpectedPrice> = HashMap::new();
        for (chain_id, assets) in traded_chains {
            for (symbol, asset) in &assets.equities.symbols {
                expected
                    .entry(format!("wt{symbol}"))
                    .or_insert_with(|| ExpectedPrice {
                        symbol: symbol.clone(),
                        traded: HashMap::new(),
                    })
                    .traded
                    .insert(chain_id, asset.tokenized_equity_derivative);
            }
        }

        Self {
            ctx,
            expected: Arc::new(expected),
            store,
            sender,
        }
    }

    async fn run_forever(&self) -> std::convert::Infallible {
        let mut expiry = interval(EXPIRY_CHECK_INTERVAL);
        expiry.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut backoff = ReconnectBackoff::default();

        loop {
            let healthy_session = match self.connect_while_expiring(&mut expiry).await {
                Ok(mut socket) => {
                    info!(target: "dashboard", "Connected to pricing service");
                    self.run_connected_session(&mut socket, &mut expiry).await
                }
                Err(error) => {
                    warn!(target: "dashboard", %error, "Pricing service connection failed");
                    false
                }
            };
            if healthy_session {
                backoff.reset();
            } else {
                backoff.record_failure();
            }

            let reconnect_delay = backoff.delay(reconnect_jitter());
            warn!(
                target: "dashboard",
                delay_ms = reconnect_delay.as_millis(),
                consecutive_failures = backoff.consecutive_failures,
                "Reconnecting to pricing service after backoff"
            );
            let reconnect = sleep(reconnect_delay);
            tokio::pin!(reconnect);
            loop {
                tokio::select! {
                    () = &mut reconnect => break,
                    _ = expiry.tick() => self.expire_prices().await,
                }
            }
        }
    }

    async fn connect_while_expiring(
        &self,
        expiry: &mut Interval,
    ) -> Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        PricingSessionError,
    > {
        let connection = self.connect_and_subscribe();
        tokio::pin!(connection);

        loop {
            tokio::select! {
                result = &mut connection => return result,
                _ = expiry.tick() => self.expire_prices().await,
            }
        }
    }

    async fn run_connected_session<S>(
        &self,
        socket: &mut tokio_tungstenite::WebSocketStream<S>,
        expiry: &mut Interval,
    ) -> bool
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        self.run_connected_session_with_timeout(socket, expiry, HEARTBEAT_TIMEOUT)
            .await
    }

    async fn run_connected_session_with_timeout<S>(
        &self,
        socket: &mut tokio_tungstenite::WebSocketStream<S>,
        expiry: &mut Interval,
        heartbeat_timeout: Duration,
    ) -> bool
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let heartbeat = sleep(heartbeat_timeout);
        tokio::pin!(heartbeat);
        let mut healthy = false;

        loop {
            tokio::select! {
                _ = expiry.tick() => self.expire_prices().await,
                () = &mut heartbeat => {
                    warn!(target: "dashboard", "Pricing WebSocket heartbeat timed out");
                    break;
                }
                incoming = socket.next() => {
                    let Some(incoming) = incoming else {
                        warn!(target: "dashboard", "Pricing WebSocket closed");
                        break;
                    };

                    match incoming {
                        Ok(message) => {
                            heartbeat.as_mut().reset(tokio::time::Instant::now() + heartbeat_timeout);
                            if let Err(error) = self.handle_message(socket, message).await {
                                warn!(target: "dashboard", %error, "Pricing WebSocket message failed");
                                break;
                            }
                            healthy = true;
                        }
                        Err(error) => {
                            warn!(target: "dashboard", %error, "Pricing WebSocket receive failed");
                            break;
                        }
                    }
                }
            }
        }

        self.make_all_unavailable().await;
        healthy
    }

    async fn connect_and_subscribe(
        &self,
    ) -> Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        PricingSessionError,
    > {
        let mut request = self
            .ctx
            .ws_url
            .as_str()
            .into_client_request()
            .map_err(PricingSessionError::Request)?;
        let bearer = match &self.ctx.auth {
            PricingAuth::ApiKey(api_key) => api_key.bearer_value().to_string(),
            // Cloud Run IAM: a fresh ID token per (re)connect from the
            // instance metadata server -- tokens live ~1h, and the
            // reconnect loop already re-enters here on every drop, so
            // expiry never needs its own timer.
            PricingAuth::GcpIdToken { audience } => fetch_gcp_identity_token(audience).await?,
        };
        let mut authorization = HeaderValue::from_str(&format!("Bearer {bearer}"))
            .map_err(PricingSessionError::AuthorizationHeader)?;
        authorization.set_sensitive(true);
        request.headers_mut().insert(AUTHORIZATION, authorization);

        let (mut socket, _) = timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(request))
            .await
            .map_err(|_| PricingSessionError::ConnectTimeout)?
            .map_err(PricingSessionError::WebSocket)?;
        let mut assets = self.expected.keys().cloned().collect::<Vec<_>>();
        assets.sort();
        let frame = ClientFrame::Subscribe(SubscribeFrame {
            consumer: CONSUMER.to_string(),
            assets,
        });
        socket
            .send(Message::binary(encode_frame(&frame)?))
            .await
            .map_err(PricingSessionError::WebSocket)?;

        Ok(socket)
    }

    async fn handle_message<S>(
        &self,
        socket: &mut tokio_tungstenite::WebSocketStream<S>,
        message: Message,
    ) -> Result<(), PricingSessionError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        match message {
            Message::Binary(bytes) => {
                let frame = ciborium::from_reader::<ServerFrame, _>(bytes.as_ref())
                    .map_err(PricingSessionError::Decode)?;

                match frame {
                    ServerFrame::Price(frame) => self.apply_frame(frame).await,
                    ServerFrame::Error(frame) => self.apply_error(frame).await,
                    ServerFrame::Halt(frame) => {
                        if let Some(expected) = self.expected.get(&frame.asset) {
                            if expected.traded.contains_key(&frame.chain_id) {
                                info!(symbol = %expected.symbol, asset = %frame.asset, halted = frame.halted,
                                    "Pricing halt status changed");
                                if frame.halted {
                                    self.set_unavailable(&expected.symbol).await;
                                }
                            } else {
                                debug!(
                                    asset = %frame.asset,
                                    chain_id = frame.chain_id,
                                    "Ignoring halt for an untraded chain"
                                );
                            }
                        } else {
                            debug!(asset = %frame.asset, "Ignoring halt for an unrequested asset");
                        }
                    }
                    ServerFrame::Ping(heartbeat) => {
                        let response = ClientFrame::Pong(PongFrame {
                            ts_unix_ms: heartbeat.ts_unix_ms,
                        });
                        socket
                            .send(Message::binary(encode_frame(&response)?))
                            .await
                            .map_err(PricingSessionError::WebSocket)?;
                    }
                }
            }
            Message::Close(_) => return Err(PricingSessionError::Closed),
            Message::Ping(payload) => socket
                .send(Message::Pong(payload))
                .await
                .map_err(PricingSessionError::WebSocket)?,
            Message::Text(_) | Message::Pong(_) | Message::Frame(_) => {}
        }

        Ok(())
    }

    async fn apply_frame(&self, frame: PriceFrame) {
        let Some(expected) = self.expected.get(&frame.asset) else {
            warn!(target: "dashboard", asset = %frame.asset, "Ignoring unrequested pricing asset");
            return;
        };

        match validated_price(&frame, expected, Utc::now()) {
            Ok(price) => {
                if !self.store.update(&expected.symbol, price.clone()).await {
                    debug!(target: "dashboard", symbol = %expected.symbol, "Ignoring older pricing quote");
                    return;
                }
                self.broadcast(EquityPrice {
                    symbol: expected.symbol.clone(),
                    status: EquityPriceStatus::Available {
                        price_usd: price.price_usd,
                        observed_at: price.observed_at,
                        expires_at: price.expires_at,
                    },
                });
            }
            Err(InvalidPrice::UntradedChain) => {
                // A quote for a chain we don't trade this symbol on. The
                // pricing service publishes the same symbol on chains we only
                // price and never trade (e.g. Robinhood); those frames are not
                // ours. Ignore -- never let one clobber the price we hold for a
                // chain we do trade.
                debug!(
                    target: "dashboard",
                    symbol = %expected.symbol,
                    chain_id = frame.chain_id,
                    "Discarding quote for a chain we don't trade this symbol on"
                );
            }
            Err(error) => {
                warn!(target: "dashboard", symbol = %expected.symbol, %error, "Rejecting pricing quote");
                self.set_unavailable(&expected.symbol).await;
            }
        }
    }

    async fn apply_error(&self, frame: ErrorFrame) {
        let Some(asset) = frame.asset else {
            warn!(target: "dashboard", code = ?frame.code, "Pricing service reported a session error");
            return;
        };
        let Some(expected) = self.expected.get(&asset) else {
            debug!(target: "dashboard", %asset, code = ?frame.code, "Pricing error for unrequested asset");
            return;
        };

        warn!(target: "dashboard", symbol = %expected.symbol, code = ?frame.code, "Pricing service marked asset unavailable");
        self.set_unavailable(&expected.symbol).await;
    }

    async fn expire_prices(&self) {
        for symbol in self.store.expire(Utc::now()).await {
            warn!(target: "dashboard", %symbol, "Dashboard price expired");
            self.broadcast(EquityPrice {
                symbol,
                status: EquityPriceStatus::Unavailable,
            });
        }
    }

    async fn set_unavailable(&self, symbol: &Symbol) {
        if self.store.make_unavailable(symbol).await {
            self.broadcast(EquityPrice {
                symbol: symbol.clone(),
                status: EquityPriceStatus::Unavailable,
            });
        }
    }

    async fn make_all_unavailable(&self) {
        for symbol in self.store.make_all_unavailable().await {
            self.broadcast(EquityPrice {
                symbol,
                status: EquityPriceStatus::Unavailable,
            });
        }
    }

    fn broadcast(&self, price: EquityPrice) {
        if self
            .sender
            .send(Statement::EquityPriceUpdate(price))
            .is_err()
        {
            debug!(target: "dashboard", "No dashboard clients subscribed to price update");
        }
    }
}

impl SupervisedTask for EquityPriceMonitor {
    async fn run(&mut self) -> TaskResult {
        match self.run_forever().await {}
    }
}

fn validated_price(
    frame: &PriceFrame,
    expected: &ExpectedPrice,
    now: DateTime<Utc>,
) -> Result<AvailablePrice, InvalidPrice> {
    // Chain membership is checked first: a frame for a chain we don't trade
    // this symbol on is not ours and must never fall through to another error
    // that would blank the price we hold for a chain we do trade.
    let Some(&base) = expected.traded.get(&frame.chain_id) else {
        return Err(InvalidPrice::UntradedChain);
    };
    if frame.venue != Venue::Raindex {
        return Err(InvalidPrice::Venue);
    }
    // The quote token must be the settlement stable of the frame's chain,
    // which differs per chain.
    let settlement_stable = Chain::ALL
        .into_iter()
        .find(|chain| chain.chain_id() == frame.chain_id)
        .map(Chain::settlement_stable)
        .ok_or(InvalidPrice::UntradedChain)?;
    if Address::from(frame.base.0) != base
        || settlement_stable.address != Address::from(frame.quote.0)
    {
        return Err(InvalidPrice::Pair { settlement_stable });
    }

    let observed_at = Utc
        .timestamp_millis_opt(frame.source_ts_unix_ms)
        .single()
        .ok_or(InvalidPrice::Timestamp)?;
    let expires_at = Utc
        .timestamp_millis_opt(frame.expiry_unix_ms)
        .single()
        .ok_or(InvalidPrice::Timestamp)?;
    if observed_at > now {
        return Err(InvalidPrice::FutureTimestamp);
    }
    if observed_at >= expires_at || expires_at <= now {
        return Err(InvalidPrice::Expired);
    }

    let price_usd = mid_price(&frame.rate_base_to_quote, &frame.rate_quote_to_base)?;

    // Both underlying rates zero is the wire sentinel for "not carried".
    let underlying_bid = Float::from_raw(B256::from(frame.underlying_rate_base_to_quote.0));
    let underlying_quote_to_base =
        Float::from_raw(B256::from(frame.underlying_rate_quote_to_base.0));
    let underlying_price_usd = if underlying_bid.is_zero()? && underlying_quote_to_base.is_zero()? {
        None
    } else {
        // Only the rebalancer reads the underlying price, so a bad pair must
        // not cost the dashboard its wrapped price.
        mid_price(
            &frame.underlying_rate_base_to_quote,
            &frame.underlying_rate_quote_to_base,
        )
        .inspect_err(|error| {
            warn!(
                target: "dashboard",
                symbol = %expected.symbol,
                %error,
                "Ignoring an invalid underlying rate pair; the symbol has no mark"
            );
        })
        .ok()
    };

    Ok(AvailablePrice {
        price_usd,
        underlying_price_usd,
        observed_at,
        expires_at,
    })
}

/// Mid of a directional rate pair, refusing non-positive or crossed rates.
fn mid_price(base_to_quote: &WireFloat, quote_to_base: &WireFloat) -> Result<Float, InvalidPrice> {
    let bid = Float::from_raw(B256::from(base_to_quote.0));
    let quote_to_base = Float::from_raw(B256::from(quote_to_base.0));
    if !bid.gt(float!(0))? || !quote_to_base.gt(float!(0))? {
        return Err(InvalidPrice::NonPositive);
    }
    let ask = (float!(1) / quote_to_base)?;
    if bid.gt(ask)? {
        return Err(InvalidPrice::Crossed);
    }

    Ok(((bid + ask)? / float!(2))?)
}

fn encode_frame<T: serde::Serialize>(
    frame: &T,
) -> Result<Vec<u8>, ciborium::ser::Error<io::Error>> {
    let mut encoded = Vec::new();
    ciborium::into_writer(frame, &mut encoded)?;
    Ok(encoded)
}

#[derive(Debug, thiserror::Error)]
enum PricingSessionError {
    #[error("invalid pricing WebSocket request")]
    Request(#[source] WebSocketError),
    #[error("pricing API key is not valid as an authorization header")]
    AuthorizationHeader(#[source] tokio_tungstenite::tungstenite::http::header::InvalidHeaderValue),
    #[error("pricing WebSocket connection timed out")]
    ConnectTimeout,
    #[error("pricing WebSocket failed")]
    WebSocket(#[source] WebSocketError),
    #[error("pricing frame could not be decoded")]
    Decode(#[source] ciborium::de::Error<io::Error>),
    #[error("pricing frame could not be encoded")]
    Encode(#[from] ciborium::ser::Error<io::Error>),
    #[error("pricing WebSocket closed")]
    Closed,
    #[error("failed to mint a Google ID token for the pricing service")]
    IdentityToken(#[source] reqwest::Error),
    #[error("metadata server answered HTTP {0} for the identity token")]
    IdentityTokenStatus(u16),
}

/// Mints a Google ID token for `audience` from the GCE instance metadata
/// server — the VM's ambient service-account identity, the same
/// no-stored-credential model as the Turnkey KMS stamper. Only reachable
/// on GCP by construction; the config layer refuses `gcp_id_token`
/// without wss, and off-GCP this endpoint simply does not resolve.
async fn fetch_gcp_identity_token(audience: &str) -> Result<String, PricingSessionError> {
    fetch_gcp_identity_token_from(crate::pricing_identity::METADATA_IDENTITY_URL, audience).await
}

async fn fetch_gcp_identity_token_from(
    base_url: &str,
    audience: &str,
) -> Result<String, PricingSessionError> {
    // no_proxy: the token must travel ONLY the direct link to the
    // metadata server — a proxy honored from HTTP_PROXY/ALL_PROXY env
    // would otherwise see a live credential (review catch). The fixed
    // transport timeout mirrors the KMS stamper's: an implementation
    // detail of a link-local endpoint, not an operational knob.
    crate::pricing_identity::fetch_identity_from(
        base_url,
        audience,
        std::time::Duration::from_secs(10),
    )
    .await
    .map_err(|error| match error {
        crate::pricing_identity::PricingIdentityError::Request(source) => {
            PricingSessionError::IdentityToken(source)
        }
        crate::pricing_identity::PricingIdentityError::Status(status) => {
            PricingSessionError::IdentityTokenStatus(status)
        }
    })
}

#[derive(Debug, thiserror::Error)]
enum InvalidPrice {
    #[error("venue is not raindex")]
    Venue,
    #[error("symbol is not traded on this chain")]
    UntradedChain,
    #[error(
        "token pair does not match configured wrapped equity and {} ({})",
        .settlement_stable.symbol,
        .settlement_stable.address
    )]
    Pair { settlement_stable: SettlementStable },
    #[error("source or expiry timestamp is invalid")]
    Timestamp,
    #[error("source timestamp is in the future")]
    FutureTimestamp,
    #[error("quote is already expired")]
    Expired,
    #[error("directional rates must be positive")]
    NonPositive,
    #[error("bid exceeds ask")]
    Crossed,
    #[error("price arithmetic failed")]
    Float(#[from] FloatError),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use chrono::TimeDelta;
    use st0x_evm::{USDC_BASE, USDC_ETHEREUM};
    use st0x_pricing_types::{ErrorCode, HaltFrame, PingFrame, WireAddress, WireFloat};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::handshake::server::{
        Callback, ErrorResponse, Request, Response,
    };
    use url::Url;

    use st0x_config::{ChainEquities, ChainEquityAsset, OperationMode, RebalancingMode};

    use super::*;

    struct AssertDashboardAuthorization;

    impl Callback for AssertDashboardAuthorization {
        fn on_request(
            self,
            request: &Request,
            response: Response,
        ) -> Result<Response, ErrorResponse> {
            assert_eq!(
                request.headers().get(AUTHORIZATION).unwrap(),
                "Bearer pricing-oracle-test-key"
            );

            Ok(response)
        }
    }

    const TEST_CHAIN_ID: u64 = 8_453;
    const TEST_DERIVATIVE: Address = address!("0x1111111111111111111111111111111111111111");

    fn expected() -> ExpectedPrice {
        ExpectedPrice {
            symbol: Symbol::new("AAPL").unwrap(),
            traded: HashMap::from([(TEST_CHAIN_ID, TEST_DERIVATIVE)]),
        }
    }

    fn wire_float(value: Float) -> WireFloat {
        WireFloat::from_bytes(value.get_inner().0)
    }

    fn frame(bid: Float, quote_to_base: Float, now: DateTime<Utc>) -> PriceFrame {
        PriceFrame {
            asset: "wtAAPL".to_string(),
            venue: Venue::Raindex,
            chain_id: TEST_CHAIN_ID,
            base: WireAddress::from_bytes(TEST_DERIVATIVE.into_array()),
            quote: WireAddress::from_bytes(USDC_BASE.into_array()),
            rate_base_to_quote: wire_float(bid),
            rate_quote_to_base: wire_float(quote_to_base),
            underlying_rate_base_to_quote: wire_float(bid),
            underlying_rate_quote_to_base: wire_float(quote_to_base),
            nav_ratio: st0x_pricing_types::WireU256::from_bytes(
                alloy::primitives::U256::from(1_000_000_000_000_000_000_u64).to_be_bytes(),
            ),
            execution_deadline_unix_ms: None,
            expiry_unix_ms: (now + TimeDelta::seconds(30)).timestamp_millis(),
            model_version: "test".to_string(),
            source_ts_unix_ms: now.timestamp_millis(),
        }
    }

    fn assets() -> ChainAssets {
        ChainAssets {
            equities: ChainEquities {
                operational_limit: None,
                symbols: HashMap::from([(
                    Symbol::new("AAPL").unwrap(),
                    ChainEquityAsset {
                        tokenized_equity: address!("0x2222222222222222222222222222222222222222"),
                        tokenized_equity_derivative: TEST_DERIVATIVE,
                        vault_ids: Vec::new(),
                        trading: OperationMode::Enabled,
                        rebalancing: RebalancingMode::Disabled,
                        wrapped_equity_recovery: OperationMode::Disabled,
                        operational_limit: None,
                        target_share: None,
                    },
                )]),
            },
            cash: None,
        }
    }

    #[test]
    fn midpoint_uses_both_directional_rates() {
        let now = Utc::now();
        let price =
            validated_price(&frame(float!(99), float!(0.01), now), &expected(), now).unwrap();

        assert_eq!(price.price_usd.format().unwrap(), "99.5");
    }

    /// A vault share worth more than one underlying share (SGOV, SPYM) is
    /// priced per wrapped share for the dashboard, while the rebalancer's mark
    /// is the underlying mid the frame carries.
    #[tokio::test]
    async fn mark_is_the_underlying_price_not_the_wrapped_one() {
        let now = Utc::now();
        let mut vault_frame = frame(float!(101), float!(0.0099), now);
        vault_frame.underlying_rate_base_to_quote = wire_float(float!(99));
        vault_frame.underlying_rate_quote_to_base = wire_float(float!(0.01));
        let price = validated_price(&vault_frame, &expected(), now).unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        let store = EquityPriceStore {
            prices: Arc::new(RwLock::new(HashMap::from([(symbol.clone(), None)]))),
            mark_listener: Arc::default(),
        };
        assert!(store.update(&symbol, price).await);

        let mark = store.mark(&symbol, now).await.unwrap();

        assert_eq!(mark.price.format().unwrap(), "99.5");
    }

    #[derive(Default)]
    struct RecordingMarkListener(tokio::sync::Mutex<Vec<Symbol>>);

    #[async_trait::async_trait]
    impl MarkListener for RecordingMarkListener {
        async fn mark_available(&self, symbol: &Symbol) {
            self.0.lock().await.push(symbol.clone());
        }
    }

    fn price_with_mark(now: DateTime<Utc>, carries_underlying: bool) -> AvailablePrice {
        let (base_to_quote, quote_to_base) = if carries_underlying {
            (float!(99), float!(0.01))
        } else {
            (float!(0), float!(0))
        };
        let mut vault_frame = frame(float!(101), float!(0.0099), now);
        vault_frame.underlying_rate_base_to_quote = wire_float(base_to_quote);
        vault_frame.underlying_rate_quote_to_base = wire_float(quote_to_base);
        validated_price(&vault_frame, &expected(), now).unwrap()
    }

    /// The listener hears a symbol once when it gains a usable mark, not on
    /// every later quote, and not for a quote without the underlying rates.
    #[tokio::test]
    async fn only_a_newly_usable_mark_notifies_the_listener() {
        let symbol = Symbol::new("AAPL").unwrap();
        let store = EquityPriceStore {
            prices: Arc::new(RwLock::new(HashMap::from([(symbol.clone(), None)]))),
            mark_listener: Arc::default(),
        };
        let listener = Arc::new(RecordingMarkListener::default());
        store.notify_marks_to(listener.clone()).await;
        let start = Utc::now();

        assert!(store.update(&symbol, price_with_mark(start, false)).await);
        assert!(listener.0.lock().await.is_empty(), "no mark yet");

        let marked = start + TimeDelta::seconds(1);
        assert!(store.update(&symbol, price_with_mark(marked, true)).await);
        let refreshed = start + TimeDelta::seconds(2);
        assert!(
            store
                .update(&symbol, price_with_mark(refreshed, true))
                .await
        );
        assert_eq!(*listener.0.lock().await, vec![symbol.clone()]);

        assert!(store.make_unavailable(&symbol).await);
        let returned = start + TimeDelta::seconds(3);
        assert!(store.update(&symbol, price_with_mark(returned, true)).await);
        assert_eq!(
            *listener.0.lock().await,
            vec![symbol.clone(), symbol],
            "a mark that comes back after an outage must notify again"
        );
    }

    /// A held mark past its expiry already reads as missing to the planner,
    /// so a fresh quote replacing it before the expiry sweep runs must notify.
    #[tokio::test]
    async fn fresh_mark_over_an_expired_unswept_one_notifies_the_listener() {
        let symbol = Symbol::new("AAPL").unwrap();
        let observed = Utc::now() - TimeDelta::seconds(60);
        let store = EquityPriceStore {
            prices: Arc::new(RwLock::new(HashMap::from([(
                symbol.clone(),
                Some(AvailablePrice {
                    expires_at: observed + TimeDelta::seconds(30),
                    ..price_with_mark(observed, true)
                }),
            )]))),
            mark_listener: Arc::default(),
        };
        let listener = Arc::new(RecordingMarkListener::default());
        store.notify_marks_to(listener.clone()).await;
        assert!(
            store.mark(&symbol, Utc::now()).await.is_none(),
            "the held mark has expired"
        );

        assert!(
            store
                .update(&symbol, price_with_mark(Utc::now(), true))
                .await
        );

        assert_eq!(*listener.0.lock().await, vec![symbol]);
    }

    /// The price monitor starts before the conductor attaches the listener,
    /// and a mark that is already usable never notifies again, so attaching
    /// replays every symbol holding a usable mark, and only those.
    #[tokio::test]
    async fn attaching_the_listener_replays_marks_that_arrived_first() {
        let now = Utc::now();
        let (marked, unpriced, priced_without_mark, expired) = (
            Symbol::new("SPY").unwrap(),
            Symbol::new("AAPL").unwrap(),
            Symbol::new("FGI").unwrap(),
            Symbol::new("SPYM").unwrap(),
        );
        let stale = now - TimeDelta::seconds(60);
        let store = EquityPriceStore {
            prices: Arc::new(RwLock::new(HashMap::from([
                (marked.clone(), Some(price_with_mark(now, true))),
                (unpriced, None),
                (priced_without_mark, Some(price_with_mark(now, false))),
                (
                    expired,
                    Some(AvailablePrice {
                        expires_at: stale + TimeDelta::seconds(30),
                        ..price_with_mark(stale, true)
                    }),
                ),
            ]))),
            mark_listener: Arc::default(),
        };
        let listener = Arc::new(RecordingMarkListener::default());

        store.notify_marks_to(listener.clone()).await;

        assert_eq!(*listener.0.lock().await, vec![marked]);
    }

    #[tokio::test]
    async fn invalid_underlying_rates_give_no_mark_but_keep_the_dashboard_price() {
        let now = Utc::now();
        let mut half_frame = frame(float!(99), float!(0.01), now);
        half_frame.underlying_rate_base_to_quote = wire_float(float!(99));
        half_frame.underlying_rate_quote_to_base = wire_float(float!(0));
        let price = validated_price(&half_frame, &expected(), now).unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        let store = EquityPriceStore {
            prices: Arc::new(RwLock::new(HashMap::from([(symbol.clone(), None)]))),
            mark_listener: Arc::default(),
        };
        assert!(store.update(&symbol, price).await);

        assert!(store.mark(&symbol, now).await.is_none());
        let EquityPriceStatus::Available { price_usd, .. } = store.snapshot(now).await[0].status
        else {
            panic!("the dashboard should keep the wrapped price")
        };
        assert_eq!(price_usd.format().unwrap(), "99.5");
    }

    /// Frames from producers that predate the underlying rates carry zero for
    /// both. The dashboard still shows the wrapped price, but the rebalancer
    /// gets no mark rather than a wrapped price posing as an underlying one.
    #[tokio::test]
    async fn frame_without_underlying_rates_gives_no_mark() {
        let now = Utc::now();
        let mut legacy_frame = frame(float!(99), float!(0.01), now);
        legacy_frame.underlying_rate_base_to_quote = wire_float(float!(0));
        legacy_frame.underlying_rate_quote_to_base = wire_float(float!(0));
        let price = validated_price(&legacy_frame, &expected(), now).unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        let store = EquityPriceStore {
            prices: Arc::new(RwLock::new(HashMap::from([(symbol.clone(), None)]))),
            mark_listener: Arc::default(),
        };
        assert!(store.update(&symbol, price).await);

        assert!(
            store.mark(&symbol, now).await.is_none(),
            "a wrapped price must not stand in for the underlying one"
        );
        let EquityPriceStatus::Available { price_usd, .. } = store.snapshot(now).await[0].status
        else {
            panic!("the dashboard should still show the wrapped price")
        };
        assert_eq!(price_usd.format().unwrap(), "99.5");
    }

    #[tokio::test]
    async fn halt_invalidates_only_requested_assets_until_a_fresh_price_arrives() {
        let assets = assets();
        let store = EquityPriceStore::new([&assets]);
        let (sender, mut receiver) = broadcast::channel(4);
        let monitor = EquityPriceMonitor::new(
            PricingCtx::new(Url::parse("ws://localhost").unwrap(), "test".into()).unwrap(),
            [(TEST_CHAIN_ID, &assets)],
            store.clone(),
            sender,
        );
        let (stream, _peer) = tokio::io::duplex(1024);
        let mut socket = tokio_tungstenite::WebSocketStream::from_raw_socket(
            stream,
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let price = frame(float!(100), float!(0.01), Utc::now());
        monitor.apply_frame(price.clone()).await;
        assert!(matches!(
            receiver.try_recv().unwrap(),
            Statement::EquityPriceUpdate(EquityPrice {
                status: EquityPriceStatus::Available { .. },
                ..
            })
        ));
        for (asset, halted, available, broadcasts) in [
            ("wtUNKNOWN", true, true, false),
            ("wtAAPL", true, false, true),
            ("wtAAPL", true, false, false),
            ("wtAAPL", false, false, false),
        ] {
            let halt = ServerFrame::Halt(HaltFrame {
                asset: asset.into(),
                chain_id: price.chain_id,
                base: price.base,
                quote: price.quote,
                halted,
                reason: None,
            });
            monitor
                .handle_message(&mut socket, Message::binary(encode_frame(&halt).unwrap()))
                .await
                .unwrap();
            assert_eq!(
                matches!(
                    store.snapshot(Utc::now()).await[0].status,
                    EquityPriceStatus::Available { .. }
                ),
                available
            );
            if broadcasts {
                assert!(matches!(
                    receiver.try_recv().unwrap(),
                    Statement::EquityPriceUpdate(EquityPrice {
                        status: EquityPriceStatus::Unavailable,
                        ..
                    })
                ));
            } else {
                assert!(matches!(
                    receiver.try_recv(),
                    Err(broadcast::error::TryRecvError::Empty)
                ));
            }
        }
        monitor
            .apply_frame(frame(float!(100), float!(0.01), Utc::now()))
            .await;
        assert!(matches!(
            store.snapshot(Utc::now()).await[0].status,
            EquityPriceStatus::Available { .. }
        ));
        assert!(matches!(
            receiver.try_recv().unwrap(),
            Statement::EquityPriceUpdate(EquityPrice {
                status: EquityPriceStatus::Available { .. },
                ..
            })
        ));
    }

    #[test]
    fn reconnect_backoff_is_bounded_and_resets_after_a_healthy_session() {
        let mut backoff = ReconnectBackoff::default();
        let jitter = Duration::from_millis(500);

        backoff.record_failure();
        assert_eq!(backoff.delay(jitter), Duration::from_millis(5_500));
        backoff.record_failure();
        assert_eq!(backoff.delay(jitter), Duration::from_millis(10_500));
        backoff.record_failure();
        backoff.record_failure();
        backoff.record_failure();
        assert_eq!(backoff.delay(jitter), Duration::from_millis(59_500));

        backoff.reset();
        assert_eq!(backoff.delay(jitter), Duration::from_millis(5_500));
    }

    #[test]
    fn crossed_quote_is_unavailable() {
        let now = Utc::now();
        let result = validated_price(&frame(float!(101), float!(0.01), now), &expected(), now);

        assert!(matches!(result, Err(InvalidPrice::Crossed)));
    }

    #[test]
    fn future_quote_cannot_poison_store_ordering() {
        let now = Utc::now();
        let mut future = frame(float!(99), float!(0.01), now);
        future.source_ts_unix_ms = (now + TimeDelta::seconds(30)).timestamp_millis();
        future.expiry_unix_ms = (now + TimeDelta::seconds(60)).timestamp_millis();

        assert!(matches!(
            validated_price(&future, &expected(), now),
            Err(InvalidPrice::FutureTimestamp)
        ));
    }

    #[test]
    fn untraded_chain_quote_is_rejected_as_untraded() {
        let now = Utc::now();
        let mut untraded = frame(float!(99), float!(0.01), now);
        // Robinhood (4663): the pricing service publishes this symbol here,
        // but the bot does not trade it on this chain.
        untraded.chain_id = 4_663;

        assert!(matches!(
            validated_price(&untraded, &expected(), now),
            Err(InvalidPrice::UntradedChain)
        ));
    }

    #[test]
    fn quote_is_accepted_on_any_chain_the_symbol_is_traded_on() {
        let now = Utc::now();
        let other_chain = Chain::Ethereum.chain_id();
        let other_derivative = address!("0x3333333333333333333333333333333333333333");
        let expected = ExpectedPrice {
            symbol: Symbol::new("AAPL").unwrap(),
            traded: HashMap::from([
                (TEST_CHAIN_ID, TEST_DERIVATIVE),
                (other_chain, other_derivative),
            ]),
        };

        assert!(validated_price(&frame(float!(99), float!(0.01), now), &expected, now).is_ok());

        let mut second = frame(float!(99), float!(0.01), now);
        second.chain_id = other_chain;
        second.base = WireAddress::from_bytes(other_derivative.into_array());
        second.quote = WireAddress::from_bytes(USDC_ETHEREUM.into_array());
        assert!(validated_price(&second, &expected, now).is_ok());
    }

    #[test]
    fn pair_error_names_the_frame_chains_settlement_stable() {
        let now = Utc::now();
        let chain = Chain::Ethereum;
        let derivative = address!("0x3333333333333333333333333333333333333333");
        let expected = ExpectedPrice {
            symbol: Symbol::new("AAPL").unwrap(),
            traded: HashMap::from([(chain.chain_id(), derivative)]),
        };
        let mut mismatched = frame(float!(99), float!(0.01), now);
        mismatched.chain_id = chain.chain_id();
        mismatched.base = WireAddress::from_bytes(derivative.into_array());

        let error = validated_price(&mismatched, &expected, now).unwrap_err();
        let settlement_stable = chain.settlement_stable();

        assert!(matches!(
            &error,
            InvalidPrice::Pair {
                settlement_stable: actual
            } if *actual == settlement_stable
        ));
        assert_eq!(
            error.to_string(),
            format!(
                "token pair does not match configured wrapped equity and {} ({})",
                settlement_stable.symbol, settlement_stable.address
            )
        );
    }

    #[tokio::test]
    async fn untraded_chain_quote_does_not_clobber_a_held_traded_price() {
        let assets = assets();
        let store = EquityPriceStore::new([&assets]);
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        // A live price on a chain we trade (Base).
        assert!(
            store
                .update(
                    &symbol,
                    AvailablePrice {
                        price_usd: float!(100),
                        underlying_price_usd: None,
                        observed_at: now,
                        expires_at: now + TimeDelta::seconds(30),
                    },
                )
                .await
        );

        let (sender, _receiver) = broadcast::channel(4);
        let monitor = EquityPriceMonitor::new(
            PricingCtx::new(
                Url::parse("ws://127.0.0.1:1").unwrap(),
                "pricing-oracle-test-key".to_string(),
            )
            .unwrap(),
            [(TEST_CHAIN_ID, &assets)],
            store.clone(),
            sender,
        );

        // A quote arrives for a chain we do NOT trade this symbol on.
        let mut untraded = frame(float!(99), float!(0.01), now);
        untraded.chain_id = 4_663;
        monitor.apply_frame(untraded).await;

        // The held Base price must survive -- never blanked by an untraded
        // chain's frame (the FGI-on-Robinhood regression).
        assert!(matches!(
            store.snapshot(Utc::now()).await[0].status,
            EquityPriceStatus::Available { .. }
        ));
    }

    #[tokio::test]
    async fn store_seeds_symbols_from_every_traded_chain() {
        let base = assets();
        let mut other = assets();
        let asset = other.equities.symbols.values().next().unwrap().clone();
        other.equities.symbols = HashMap::from([(Symbol::new("TSLA").unwrap(), asset)]);

        let store = EquityPriceStore::new([&base, &other]);

        let symbols: Vec<Symbol> = store
            .snapshot(Utc::now())
            .await
            .into_iter()
            .map(|price| price.symbol)
            .collect();
        assert!(symbols.contains(&Symbol::new("AAPL").unwrap()));
        assert!(symbols.contains(&Symbol::new("TSLA").unwrap()));
    }

    #[tokio::test]
    async fn expired_store_value_projects_as_unavailable() {
        let symbol = Symbol::new("AAPL").unwrap();
        let store = EquityPriceStore {
            prices: Arc::new(RwLock::new(HashMap::from([(
                symbol.clone(),
                Some(AvailablePrice {
                    price_usd: float!(100),
                    underlying_price_usd: None,
                    observed_at: Utc::now() - TimeDelta::seconds(60),
                    expires_at: Utc::now() - TimeDelta::seconds(30),
                }),
            )]))),
            mark_listener: Arc::default(),
        };

        let snapshot = store.snapshot(Utc::now()).await;

        assert_eq!(snapshot.len(), 1);
        assert!(matches!(snapshot[0].status, EquityPriceStatus::Unavailable));
    }

    #[tokio::test]
    async fn mark_is_the_live_price_until_it_expires() {
        let symbol = Symbol::new("SPY").unwrap();
        let store = EquityPriceStore::with_live_mark(symbol.clone(), float!(766.59));
        let now = Utc::now();

        let mark = store.mark(&symbol, now).await.unwrap();
        assert_eq!(mark.price.format().unwrap(), "766.59");

        assert!(
            store
                .mark(&symbol, now + TimeDelta::seconds(31))
                .await
                .is_none(),
            "an expired mark must not price the symbol"
        );
        assert!(
            store
                .mark(&Symbol::new("AAPL").unwrap(), now)
                .await
                .is_none(),
            "a symbol the store does not track has no mark"
        );
    }

    #[tokio::test]
    async fn older_quote_cannot_replace_a_newer_price() {
        let symbol = Symbol::new("AAPL").unwrap();
        let store = EquityPriceStore {
            prices: Arc::new(RwLock::new(HashMap::from([(symbol.clone(), None)]))),
            mark_listener: Arc::default(),
        };
        let now = Utc::now();
        let newer = AvailablePrice {
            price_usd: float!(101),
            underlying_price_usd: None,
            observed_at: now,
            expires_at: now + TimeDelta::seconds(30),
        };
        let older = AvailablePrice {
            price_usd: float!(99),
            underlying_price_usd: None,
            observed_at: now - TimeDelta::seconds(1),
            expires_at: now + TimeDelta::seconds(30),
        };

        assert!(store.update(&symbol, newer).await);
        assert!(!store.update(&symbol, older).await);
        let snapshot = store.snapshot(now).await;
        let EquityPriceStatus::Available { price_usd, .. } = snapshot[0].status else {
            panic!("newer price should remain available")
        };
        assert_eq!(price_usd.format().unwrap(), "101");
    }

    #[tokio::test]
    async fn equal_timestamp_replay_cannot_replace_or_extend_a_price() {
        let symbol = Symbol::new("AAPL").unwrap();
        let store = EquityPriceStore {
            prices: Arc::new(RwLock::new(HashMap::from([(symbol.clone(), None)]))),
            mark_listener: Arc::default(),
        };
        let now = Utc::now();
        let original_expiry = now + TimeDelta::seconds(30);
        assert!(
            store
                .update(
                    &symbol,
                    AvailablePrice {
                        price_usd: float!(101),
                        underlying_price_usd: None,
                        observed_at: now,
                        expires_at: original_expiry,
                    },
                )
                .await
        );

        assert!(
            !store
                .update(
                    &symbol,
                    AvailablePrice {
                        price_usd: float!(99),
                        underlying_price_usd: None,
                        observed_at: now,
                        expires_at: now + TimeDelta::seconds(60),
                    },
                )
                .await
        );
        let snapshot = store.snapshot(now).await;
        let EquityPriceStatus::Available {
            price_usd,
            expires_at,
            ..
        } = snapshot[0].status
        else {
            panic!("original price should remain available")
        };
        assert_eq!(price_usd.format().unwrap(), "101");
        assert_eq!(expires_at, original_expiry);
    }

    #[tokio::test]
    async fn subscription_authenticates_and_requests_wrapped_raindex_assets() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_hdr_async(stream, AssertDashboardAuthorization)
                .await
                .unwrap();
            let Message::Binary(frame) = socket.next().await.unwrap().unwrap() else {
                panic!("subscription should be a binary CBOR frame")
            };

            ciborium::from_reader::<ClientFrame, _>(frame.as_ref()).unwrap()
        });
        let assets = assets();
        let store = EquityPriceStore::new([&assets]);
        let (sender, _) = broadcast::channel(4);
        let monitor = EquityPriceMonitor::new(
            PricingCtx::new(
                Url::parse(&format!("ws://{address}")).unwrap(),
                "pricing-oracle-test-key".to_string(),
            )
            .unwrap(),
            [(TEST_CHAIN_ID, &assets)],
            store,
            sender,
        );

        let socket = monitor.connect_and_subscribe().await.unwrap();
        let subscribed = server.await.unwrap();
        drop(socket);

        let ClientFrame::Subscribe(subscribed) = subscribed else {
            panic!("first client frame should subscribe")
        };
        assert_eq!(subscribed.consumer, CONSUMER);
        assert_eq!(subscribed.assets, vec!["wtAAPL"]);
    }

    #[tokio::test]
    async fn disconnect_makes_current_prices_unavailable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_hdr_async(stream, AssertDashboardAuthorization)
                .await
                .unwrap();
            let Message::Binary(_) = socket.next().await.unwrap().unwrap() else {
                panic!("subscription should be a binary CBOR frame")
            };
            socket.close(None).await.unwrap();
        });
        let assets = assets();
        let store = EquityPriceStore::new([&assets]);
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        assert!(
            store
                .update(
                    &symbol,
                    AvailablePrice {
                        price_usd: float!(100),
                        underlying_price_usd: None,
                        observed_at: now,
                        expires_at: now + TimeDelta::seconds(30),
                    },
                )
                .await
        );
        let (sender, mut receiver) = broadcast::channel(4);
        let monitor = EquityPriceMonitor::new(
            PricingCtx::new(
                Url::parse(&format!("ws://{address}")).unwrap(),
                "pricing-oracle-test-key".to_string(),
            )
            .unwrap(),
            [(TEST_CHAIN_ID, &assets)],
            store.clone(),
            sender,
        );
        let mut socket = monitor.connect_and_subscribe().await.unwrap();
        let mut expiry = interval(EXPIRY_CHECK_INTERVAL);

        let healthy = monitor
            .run_connected_session(&mut socket, &mut expiry)
            .await;
        server.await.unwrap();

        assert!(!healthy);
        assert!(matches!(
            store.snapshot(Utc::now()).await[0].status,
            EquityPriceStatus::Unavailable
        ));
        assert!(matches!(
            receiver.recv().await.unwrap(),
            Statement::EquityPriceUpdate(EquityPrice {
                status: EquityPriceStatus::Unavailable,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn silent_socket_timeout_makes_current_prices_unavailable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_hdr_async(stream, AssertDashboardAuthorization)
                .await
                .unwrap();
            let Message::Binary(_) = socket.next().await.unwrap().unwrap() else {
                panic!("subscription should be a binary CBOR frame")
            };
            let _ = socket.next().await;
        });
        let assets = assets();
        let store = EquityPriceStore::new([&assets]);
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        assert!(
            store
                .update(
                    &symbol,
                    AvailablePrice {
                        price_usd: float!(100),
                        underlying_price_usd: None,
                        observed_at: now,
                        expires_at: now + TimeDelta::seconds(30),
                    },
                )
                .await
        );
        let (sender, mut receiver) = broadcast::channel(4);
        let monitor = EquityPriceMonitor::new(
            PricingCtx::new(
                Url::parse(&format!("ws://{address}")).unwrap(),
                "pricing-oracle-test-key".to_string(),
            )
            .unwrap(),
            [(TEST_CHAIN_ID, &assets)],
            store.clone(),
            sender,
        );
        let mut socket = monitor.connect_and_subscribe().await.unwrap();
        let mut expiry = interval(EXPIRY_CHECK_INTERVAL);

        let healthy = monitor
            .run_connected_session_with_timeout(&mut socket, &mut expiry, Duration::from_millis(20))
            .await;
        drop(socket);
        server.await.unwrap();

        assert!(!healthy);
        assert!(matches!(
            store.snapshot(Utc::now()).await[0].status,
            EquityPriceStatus::Unavailable
        ));
        assert!(matches!(
            receiver.recv().await.unwrap(),
            Statement::EquityPriceUpdate(EquityPrice {
                status: EquityPriceStatus::Unavailable,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn service_ping_receives_protocol_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_hdr_async(stream, AssertDashboardAuthorization)
                .await
                .unwrap();
            let Message::Binary(_) = socket.next().await.unwrap().unwrap() else {
                panic!("subscription should be a binary CBOR frame")
            };
            socket
                .send(Message::binary(
                    encode_frame(&ServerFrame::Ping(PingFrame { ts_unix_ms: 42 })).unwrap(),
                ))
                .await
                .unwrap();
            let Message::Binary(response) = socket.next().await.unwrap().unwrap() else {
                panic!("pong should be a binary CBOR frame")
            };

            ciborium::from_reader::<ClientFrame, _>(response.as_ref()).unwrap()
        });
        let assets = assets();
        let store = EquityPriceStore::new([&assets]);
        let (sender, _) = broadcast::channel(4);
        let monitor = EquityPriceMonitor::new(
            PricingCtx::new(
                Url::parse(&format!("ws://{address}")).unwrap(),
                "pricing-oracle-test-key".to_string(),
            )
            .unwrap(),
            [(TEST_CHAIN_ID, &assets)],
            store,
            sender,
        );
        let mut socket = monitor.connect_and_subscribe().await.unwrap();
        let mut expiry = interval(EXPIRY_CHECK_INTERVAL);

        let healthy = monitor
            .run_connected_session(&mut socket, &mut expiry)
            .await;
        let response = server.await.unwrap();

        assert!(healthy);
        let ClientFrame::Pong(response) = response else {
            panic!("pricing heartbeat should receive a protocol pong")
        };
        assert_eq!(response.ts_unix_ms, 42);
    }

    #[tokio::test]
    async fn service_errors_only_invalidate_requested_assets() {
        let assets = assets();
        let store = EquityPriceStore::new([&assets]);
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        assert!(
            store
                .update(
                    &symbol,
                    AvailablePrice {
                        price_usd: float!(100),
                        underlying_price_usd: None,
                        observed_at: now,
                        expires_at: now + TimeDelta::seconds(30),
                    },
                )
                .await
        );
        let (sender, mut receiver) = broadcast::channel(4);
        let monitor = EquityPriceMonitor::new(
            PricingCtx::new(
                Url::parse("wss://pricing.test/ws").unwrap(),
                "pricing-oracle-test-key".to_string(),
            )
            .unwrap(),
            [(TEST_CHAIN_ID, &assets)],
            store.clone(),
            sender,
        );

        monitor
            .apply_error(ErrorFrame {
                code: ErrorCode::StaleSource,
                asset: None,
                last_ok_unix_ms: None,
                detail: None,
            })
            .await;
        monitor
            .apply_error(ErrorFrame {
                code: ErrorCode::UnknownAsset,
                asset: Some("wtTSLA".to_string()),
                last_ok_unix_ms: None,
                detail: None,
            })
            .await;
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        monitor
            .apply_error(ErrorFrame {
                code: ErrorCode::StaleSource,
                asset: Some("wtAAPL".to_string()),
                last_ok_unix_ms: Some(now.timestamp_millis()),
                detail: None,
            })
            .await;

        assert!(matches!(
            store.snapshot(Utc::now()).await[0].status,
            EquityPriceStatus::Unavailable
        ));
        assert!(matches!(
            receiver.recv().await.unwrap(),
            Statement::EquityPriceUpdate(EquityPrice {
                status: EquityPriceStatus::Unavailable,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn identity_token_fetch_sends_metadata_flavor_and_returns_body() {
        let server = httpmock::MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET)
                    .path("/identity")
                    .query_param("audience", "https://pricing.example")
                    .header("Metadata-Flavor", "Google");
                then.status(200).body("header.payload.signature");
            })
            .await;

        let token =
            fetch_gcp_identity_token_from(&server.url("/identity"), "https://pricing.example")
                .await
                .expect("token");

        mock.assert_async().await;
        assert_eq!(token, "header.payload.signature");
    }

    #[tokio::test]
    async fn identity_token_fetch_surfaces_non_success_status() {
        let server = httpmock::MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/identity");
                then.status(403).body("denied");
            })
            .await;

        let err = fetch_gcp_identity_token_from(&server.url("/identity"), "aud")
            .await
            .expect_err("must fail");
        assert!(matches!(err, PricingSessionError::IdentityTokenStatus(403)));
    }

    #[tokio::test]
    async fn identity_token_fetch_surfaces_transport_failure() {
        // A port nothing listens on: connection refused, mapped to
        // IdentityToken rather than a status error.
        let err = fetch_gcp_identity_token_from("http://127.0.0.1:1/identity", "aud")
            .await
            .expect_err("must fail");
        assert!(matches!(err, PricingSessionError::IdentityToken(_)));
    }
}
