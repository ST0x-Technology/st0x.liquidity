//! Live pricing-service subscription and dashboard price read model.

use alloy::primitives::{Address, B256};
use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use rain_math_float::{Float, FloatError};
use rand::Rng;
use st0x_pricing_types::{
    ClientFrame, ErrorFrame, PongFrame, PriceFrame, ServerFrame, SubscribeFrame, Venue,
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

use st0x_config::{AssetsConfig, PricingAuth, PricingCtx};
use st0x_dto::{EquityPrice, EquityPriceStatus, Statement};
use st0x_evm::USDC_BASE;
use st0x_finance::Symbol;
use st0x_float_macro::float;

const BASE_CHAIN_ID: u64 = 8_453;
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
    base: Address,
}

#[derive(Clone, Debug)]
struct AvailablePrice {
    price_usd: Float,
    observed_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

/// Process-local latest-price view used only by dashboard projections.
#[derive(Clone, Debug)]
pub(crate) struct EquityPriceStore {
    prices: Arc<RwLock<HashMap<Symbol, Option<AvailablePrice>>>>,
}

impl EquityPriceStore {
    pub(crate) fn new(assets: &AssetsConfig) -> Self {
        let prices = assets
            .equities
            .symbols
            .keys()
            .cloned()
            .map(|symbol| (symbol, None))
            .collect();

        Self {
            prices: Arc::new(RwLock::new(prices)),
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

        *value = Some(price);
        drop(prices);
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
    pub(crate) fn new(
        ctx: PricingCtx,
        assets: &AssetsConfig,
        store: EquityPriceStore,
        sender: broadcast::Sender<Statement>,
    ) -> Self {
        let expected = assets
            .equities
            .symbols
            .iter()
            .map(|(symbol, asset)| {
                (
                    format!("wt{symbol}"),
                    ExpectedPrice {
                        symbol: symbol.clone(),
                        base: asset.tokenized_equity_derivative,
                    },
                )
            })
            .collect();

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
            PricingAuth::GcpIdToken { audience } => {
                fetch_gcp_identity_token(audience).await?
            }
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
    if frame.venue != Venue::Raindex {
        return Err(InvalidPrice::Venue);
    }
    if frame.chain_id != BASE_CHAIN_ID {
        return Err(InvalidPrice::Chain);
    }
    if Address::from(frame.base.0) != expected.base || Address::from(frame.quote.0) != USDC_BASE {
        return Err(InvalidPrice::Pair);
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

    let bid = Float::from_raw(B256::from(frame.rate_base_to_quote.0));
    let quote_to_base = Float::from_raw(B256::from(frame.rate_quote_to_base.0));
    if !bid.gt(float!(0))? || !quote_to_base.gt(float!(0))? {
        return Err(InvalidPrice::NonPositive);
    }
    let ask = (float!(1) / quote_to_base)?;
    if bid.gt(ask)? {
        return Err(InvalidPrice::Crossed);
    }
    let price_usd = ((bid + ask)? / float!(2))?;

    Ok(AvailablePrice {
        price_usd,
        observed_at,
        expires_at,
    })
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
    let url = format!(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience={audience}"
    );
    let response = reqwest::Client::new()
        .get(&url)
        .header("Metadata-Flavor", "Google")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(PricingSessionError::IdentityToken)?;
    if !response.status().is_success() {
        return Err(PricingSessionError::IdentityTokenStatus(
            response.status().as_u16(),
        ));
    }
    response
        .text()
        .await
        .map_err(PricingSessionError::IdentityToken)
}

#[derive(Debug, thiserror::Error)]
enum InvalidPrice {
    #[error("venue is not raindex")]
    Venue,
    #[error("chain is not Base")]
    Chain,
    #[error("token pair does not match configured wrapped equity and Base USDC")]
    Pair,
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
    use st0x_pricing_types::{ErrorCode, PingFrame, WireAddress, WireFloat};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::handshake::server::{
        Callback, ErrorResponse, Request, Response,
    };
    use url::Url;

    use st0x_config::{EquitiesConfig, EquityAssetConfig, OperationMode};

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

    fn expected() -> ExpectedPrice {
        ExpectedPrice {
            symbol: Symbol::new("AAPL").unwrap(),
            base: address!("0x1111111111111111111111111111111111111111"),
        }
    }

    fn wire_float(value: Float) -> WireFloat {
        WireFloat::from_bytes(value.get_inner().0)
    }

    fn frame(bid: Float, quote_to_base: Float, now: DateTime<Utc>) -> PriceFrame {
        PriceFrame {
            asset: "wtAAPL".to_string(),
            venue: Venue::Raindex,
            chain_id: BASE_CHAIN_ID,
            base: WireAddress::from_bytes(expected().base.into_array()),
            quote: WireAddress::from_bytes(USDC_BASE.into_array()),
            rate_base_to_quote: wire_float(bid),
            rate_quote_to_base: wire_float(quote_to_base),
            expiry_unix_ms: (now + TimeDelta::seconds(30)).timestamp_millis(),
            model_version: "test".to_string(),
            source_ts_unix_ms: now.timestamp_millis(),
        }
    }

    fn assets() -> AssetsConfig {
        AssetsConfig {
            equities: EquitiesConfig {
                operational_limit: None,
                symbols: HashMap::from([(
                    Symbol::new("AAPL").unwrap(),
                    EquityAssetConfig {
                        tokenized_equity: address!("0x2222222222222222222222222222222222222222"),
                        tokenized_equity_derivative: expected().base,
                        pyth_feed_id: None,
                        vault_ids: Vec::new(),
                        trading: OperationMode::Enabled,
                        rebalancing: OperationMode::Disabled,
                        wrapped_equity_recovery: OperationMode::Disabled,
                        extended_hours_counter_trading: OperationMode::Disabled,
                        operational_limit: None,
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

    #[tokio::test]
    async fn expired_store_value_projects_as_unavailable() {
        let symbol = Symbol::new("AAPL").unwrap();
        let store = EquityPriceStore {
            prices: Arc::new(RwLock::new(HashMap::from([(
                symbol.clone(),
                Some(AvailablePrice {
                    price_usd: float!(100),
                    observed_at: Utc::now() - TimeDelta::seconds(60),
                    expires_at: Utc::now() - TimeDelta::seconds(30),
                }),
            )]))),
        };

        let snapshot = store.snapshot(Utc::now()).await;

        assert_eq!(snapshot.len(), 1);
        assert!(matches!(snapshot[0].status, EquityPriceStatus::Unavailable));
    }

    #[tokio::test]
    async fn older_quote_cannot_replace_a_newer_price() {
        let symbol = Symbol::new("AAPL").unwrap();
        let store = EquityPriceStore {
            prices: Arc::new(RwLock::new(HashMap::from([(symbol.clone(), None)]))),
        };
        let now = Utc::now();
        let newer = AvailablePrice {
            price_usd: float!(101),
            observed_at: now,
            expires_at: now + TimeDelta::seconds(30),
        };
        let older = AvailablePrice {
            price_usd: float!(99),
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
        };
        let now = Utc::now();
        let original_expiry = now + TimeDelta::seconds(30);
        assert!(
            store
                .update(
                    &symbol,
                    AvailablePrice {
                        price_usd: float!(101),
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
        let store = EquityPriceStore::new(&assets);
        let (sender, _) = broadcast::channel(4);
        let monitor = EquityPriceMonitor::new(
            PricingCtx::new(
                Url::parse(&format!("ws://{address}")).unwrap(),
                "pricing-oracle-test-key".to_string(),
            )
            .unwrap(),
            &assets,
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
        let store = EquityPriceStore::new(&assets);
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        assert!(
            store
                .update(
                    &symbol,
                    AvailablePrice {
                        price_usd: float!(100),
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
            &assets,
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
        let store = EquityPriceStore::new(&assets);
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        assert!(
            store
                .update(
                    &symbol,
                    AvailablePrice {
                        price_usd: float!(100),
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
            &assets,
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
        let store = EquityPriceStore::new(&assets);
        let (sender, _) = broadcast::channel(4);
        let monitor = EquityPriceMonitor::new(
            PricingCtx::new(
                Url::parse(&format!("ws://{address}")).unwrap(),
                "pricing-oracle-test-key".to_string(),
            )
            .unwrap(),
            &assets,
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
        let store = EquityPriceStore::new(&assets);
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        assert!(
            store
                .update(
                    &symbol,
                    AvailablePrice {
                        price_usd: float!(100),
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
            &assets,
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
}
