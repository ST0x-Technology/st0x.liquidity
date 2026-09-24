//! Liquidity's executor and counter-trade policy over the shared Alpaca client.

use async_trait::async_trait;
use std::fmt;
use tracing::debug;
use uuid::Uuid;

use st0x_alpaca::broker as shared;

use super::{
    AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiError, AlpacaLimitOrder, AssetDetails,
};
use crate::{
    BuyingPowerReservationCents, CancellationOutcome, ClientOrderId, CounterTradePreflight,
    CounterTradeSkipReason, Direction, EquityPosition, Executor, ExecutorOrderId, FractionalShares,
    HedgeFloor, IndicativeQuote, Inventory, InventoryResult, LatestQuote, LimitOrder, MarketOrder,
    MarketSession, MarketSessionStatus, OrderFailureTerminality, OrderPlacement, OrderState,
    Positive, PostCloseGap, RecoveredOrderPlacement, SupportedExecutor, Symbol, TryIntoExecutor,
    Usd, resolve_buy_preflight,
};

pub struct AlpacaBrokerApi {
    inner: shared::AlpacaBrokerApi,
    counter_trade_slippage_bps: u16,
    hedge_floor: HedgeFloor,
}

impl Clone for AlpacaBrokerApi {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            counter_trade_slippage_bps: self.counter_trade_slippage_bps,
            hedge_floor: self.hedge_floor.clone(),
        }
    }
}

impl fmt::Debug for AlpacaBrokerApi {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaBrokerApi")
            .field("inner", &self.inner)
            .field(
                "counter_trade_slippage_bps",
                &self.counter_trade_slippage_bps,
            )
            .field("hedge_floor", &self.hedge_floor)
            .finish_non_exhaustive()
    }
}

fn shared_direction(direction: Direction) -> shared::Direction {
    match direction {
        Direction::Buy => shared::Direction::Buy,
        Direction::Sell => shared::Direction::Sell,
    }
}

fn direction(direction: shared::Direction) -> Direction {
    match direction {
        shared::Direction::Buy => Direction::Buy,
        shared::Direction::Sell => Direction::Sell,
    }
}

fn shared_client_id(id: &ClientOrderId) -> shared::ClientOrderId {
    match id {
        ClientOrderId::Automated(uuid) => shared::ClientOrderId::from_uuid(*uuid),
        ClientOrderId::Cli(uuid) => shared::ClientOrderId::cli(*uuid),
    }
}

fn shared_market_order(order: &MarketOrder) -> shared::MarketOrder {
    shared::MarketOrder {
        symbol: order.symbol.clone(),
        shares: order.shares,
        direction: shared_direction(order.direction),
        client_order_id: shared_client_id(&order.client_order_id),
    }
}

fn order_placement(placement: shared::OrderPlacement<String>) -> OrderPlacement<String> {
    OrderPlacement {
        order_id: placement.order_id,
        symbol: placement.symbol,
        shares: placement.shares,
        direction: direction(placement.direction),
        placed_at: placement.placed_at,
        extended_hours: placement.extended_hours,
        limit_price: placement.limit_price,
    }
}

fn recovered_placement(
    placement: shared::RecoveredOrderPlacement<String>,
) -> RecoveredOrderPlacement<String> {
    RecoveredOrderPlacement {
        order_id: placement.order_id,
        symbol: placement.symbol,
        shares: placement.shares,
        direction: direction(placement.direction),
        placed_at: placement.placed_at,
        extended_hours: placement.extended_hours,
        limit_price: placement.limit_price,
    }
}

fn terminality(value: shared::OrderFailureTerminality) -> OrderFailureTerminality {
    match value {
        shared::OrderFailureTerminality::Terminal => OrderFailureTerminality::Terminal,
        shared::OrderFailureTerminality::NotTerminal => OrderFailureTerminality::NotTerminal,
    }
}

fn order_state(state: shared::OrderState) -> OrderState {
    match state {
        shared::OrderState::Pending => OrderState::Pending,
        shared::OrderState::Submitted { order_id } => OrderState::Submitted {
            order_id: ExecutorOrderId::new(order_id.as_ref()),
        },
        shared::OrderState::PartiallyFilled {
            order_id,
            shares_filled,
            avg_price,
            partially_filled_at,
        } => OrderState::PartiallyFilled {
            order_id: ExecutorOrderId::new(order_id.as_ref()),
            shares_filled,
            avg_price,
            partially_filled_at,
        },
        shared::OrderState::Filled {
            executed_at,
            order_id,
            shares_filled,
            price,
        } => OrderState::Filled {
            executed_at,
            order_id: ExecutorOrderId::new(order_id.as_ref()),
            shares_filled,
            price,
        },
        shared::OrderState::Cancelled {
            cancelled_at,
            order_id,
            shares_filled,
            avg_price,
        } => OrderState::Cancelled {
            cancelled_at,
            order_id: ExecutorOrderId::new(order_id.as_ref()),
            shares_filled,
            avg_price,
        },
        shared::OrderState::Failed {
            failed_at,
            error_reason,
            shares_filled,
            avg_price,
            terminality: source_terminality,
        } => OrderState::Failed {
            failed_at,
            error_reason,
            shares_filled,
            avg_price,
            terminality: terminality(source_terminality),
        },
    }
}

fn market_session(value: shared::MarketSession) -> MarketSession {
    match value {
        shared::MarketSession::Regular => MarketSession::Regular,
        shared::MarketSession::Extended => MarketSession::Extended,
        shared::MarketSession::Overnight => MarketSession::Overnight,
        shared::MarketSession::Closed => MarketSession::Closed,
    }
}

fn post_close_gap(value: shared::PostCloseGap) -> PostCloseGap {
    match value {
        shared::PostCloseGap::OrdinaryOvernight => PostCloseGap::OrdinaryOvernight,
        shared::PostCloseGap::MultiDayClosure => PostCloseGap::MultiDayClosure,
        shared::PostCloseGap::Unknown => PostCloseGap::Unknown,
        shared::PostCloseGap::Unavailable => PostCloseGap::Unavailable,
    }
}

fn session_status(value: shared::MarketSessionStatus) -> MarketSessionStatus {
    MarketSessionStatus {
        session: market_session(value.session),
        session_opens_at: value.session_opens_at,
        regular_session_closes_at: value.regular_session_closes_at,
        extended_session_closes_at: value.extended_session_closes_at,
        post_close_gap: post_close_gap(value.post_close_gap),
    }
}

fn inventory(value: shared::Inventory) -> Inventory {
    Inventory {
        positions: value
            .positions
            .into_iter()
            .map(|position| EquityPosition {
                symbol: position.symbol,
                quantity: position.quantity,
                market_value: position.market_value,
            })
            .collect(),
        alpaca_usdc: value.alpaca_usdc,
        usd_balance_cents: value.usd_balance_cents,
        cash_buying_power_cents: value.cash_buying_power_cents,
        cash_withdrawable_cents: value.cash_withdrawable_cents,
    }
}

fn latest_quote(value: shared::LatestQuote) -> Result<LatestQuote, AlpacaBrokerApiError> {
    LatestQuote::new(value.bid(), value.ask()).map_err(AlpacaBrokerApiError::QuoteInvariant)
}

#[async_trait]
impl Executor for AlpacaBrokerApi {
    type Error = AlpacaBrokerApiError;
    type OrderId = String;
    type Ctx = AlpacaBrokerApiCtx;

    async fn try_from_ctx(ctx: Self::Ctx) -> Result<Self, Self::Error> {
        let inner = shared::AlpacaBrokerApi::try_from_ctx(ctx.to_shared()).await?;
        Ok(Self {
            inner,
            counter_trade_slippage_bps: ctx.counter_trade_slippage_bps,
            hedge_floor: ctx.hedge_floor,
        })
    }

    async fn is_market_open(&self) -> Result<bool, Self::Error> {
        Ok(self.inner.is_market_open().await?)
    }

    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
        Ok(order_placement(
            self.inner
                .place_market_order(shared_market_order(&order))
                .await?,
        ))
    }

    async fn recover_order_by_client_id(
        &self,
        order: &MarketOrder,
    ) -> Result<Option<OrderPlacement<Self::OrderId>>, Self::Error> {
        Ok(self
            .inner
            .recover_order_by_client_id(&shared_market_order(order))
            .await?
            .map(order_placement))
    }

    async fn get_order_status(&self, order_id: &Self::OrderId) -> Result<OrderState, Self::Error> {
        Ok(order_state(self.inner.get_order_status(order_id).await?))
    }

    async fn get_order_by_client_order_id(
        &self,
        id: &ClientOrderId,
    ) -> Result<Option<RecoveredOrderPlacement<Self::OrderId>>, Self::Error> {
        Ok(self
            .inner
            .get_order_by_client_order_id(&shared_client_id(id))
            .await?
            .map(recovered_placement))
    }

    fn to_supported_executor(&self) -> SupportedExecutor {
        SupportedExecutor::AlpacaBrokerApi
    }

    fn parse_order_id(&self, order_id: &str) -> Result<Self::OrderId, Self::Error> {
        Ok(self.inner.parse_order_id(order_id)?)
    }

    async fn get_inventory(&self) -> Result<InventoryResult, Self::Error> {
        Ok(InventoryResult::Fetched(inventory(
            self.inner.fetch_inventory().await?,
        )))
    }

    async fn preflight_counter_trade(
        &self,
        order: MarketOrder,
    ) -> Result<CounterTradePreflight, Self::Error> {
        self.preflight_counter_trade_with_reserved_buying_power(
            order,
            BuyingPowerReservationCents::ZERO,
        )
        .await
    }

    async fn preflight_counter_trade_with_reserved_buying_power(
        &self,
        order: MarketOrder,
        reserved: BuyingPowerReservationCents,
    ) -> Result<CounterTradePreflight, Self::Error> {
        self.preflight(order, None, reserved).await
    }

    async fn preflight_counter_trade_at_price(
        &self,
        order: MarketOrder,
        limit_price: Positive<Usd>,
    ) -> Result<CounterTradePreflight, Self::Error> {
        self.preflight_counter_trade_at_price_with_reserved_buying_power(
            order,
            limit_price,
            BuyingPowerReservationCents::ZERO,
        )
        .await
    }

    async fn preflight_counter_trade_at_price_with_reserved_buying_power(
        &self,
        order: MarketOrder,
        limit_price: Positive<Usd>,
        reserved: BuyingPowerReservationCents,
    ) -> Result<CounterTradePreflight, Self::Error> {
        self.preflight(order, Some(limit_price), reserved).await
    }

    async fn fetch_position_mark(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<Positive<Usd>>, Self::Error> {
        Ok(self.inner.fetch_position_mark(symbol).await?)
    }

    async fn fetch_latest_quote(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<LatestQuote>, Self::Error> {
        Ok(Some(latest_quote(
            self.inner.fetch_latest_quote(symbol).await?,
        )?))
    }

    async fn market_session(&self) -> Result<MarketSession, Self::Error> {
        Ok(market_session(self.inner.market_session().await?))
    }

    async fn market_session_status(&self) -> Result<MarketSessionStatus, Self::Error> {
        Ok(session_status(self.inner.market_session_status().await?))
    }

    async fn place_limit_order(
        &self,
        order: LimitOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
        let shared_order = shared::LimitOrder {
            symbol: order.symbol,
            shares: order.shares,
            direction: shared_direction(order.direction),
            limit_price: order.limit_price,
            extended_hours: order.extended_hours,
            client_order_id: shared_client_id(&order.client_order_id),
        };
        Ok(order_placement(
            self.inner.place_limit_order(shared_order).await?,
        ))
    }

    async fn cancel_order(
        &self,
        order_id: &Self::OrderId,
    ) -> Result<CancellationOutcome, Self::Error> {
        Ok(match self.inner.cancel_order(order_id).await? {
            shared::CancellationOutcome::Requested => CancellationOutcome::Requested,
            shared::CancellationOutcome::OrderNotFound => CancellationOutcome::OrderNotFound,
        })
    }
}

#[async_trait]
impl TryIntoExecutor for AlpacaBrokerApiCtx {
    type Executor = AlpacaBrokerApi;

    async fn try_into_executor(
        self,
    ) -> Result<Self::Executor, <Self::Executor as Executor>::Error> {
        AlpacaBrokerApi::try_from_ctx(self).await
    }
}

impl AlpacaBrokerApi {
    async fn preflight(
        &self,
        mut order: MarketOrder,
        limit_price: Option<Positive<Usd>>,
        reserved: BuyingPowerReservationCents,
    ) -> Result<CounterTradePreflight, AlpacaBrokerApiError> {
        let requested = order.shares;
        let prepared = self
            .inner
            .prepare_counter_trade_shares(&order.symbol, requested, limit_price.is_some())
            .await?;
        let Some(shares) = prepared.shares else {
            return Ok(CounterTradePreflight::Skipped(
                CounterTradeSkipReason::NonFractionableQuantityBelowOne {
                    symbol: order.symbol,
                    requested,
                },
            ));
        };
        order.shares = shares;
        match order.direction {
            Direction::Sell => self.preflight_sell_inventory(order, prepared).await,
            Direction::Buy => {
                self.preflight_buy_cash(&order, limit_price, prepared.quantity_decimals, reserved)
                    .await
            }
        }
    }

    async fn preflight_sell_inventory(
        &self,
        order: MarketOrder,
        prepared: shared::PreparedShares,
    ) -> Result<CounterTradePreflight, AlpacaBrokerApiError> {
        let available = self
            .inner
            .fetch_inventory()
            .await?
            .positions
            .into_iter()
            .find(|position| position.symbol == order.symbol)
            .map_or(FractionalShares::ZERO, |position| position.quantity);
        let floor = self.hedge_floor.for_symbol(&order.symbol);
        let (tradable_available, floor) = if prepared.fractional_orders_supported {
            (available, floor)
        } else {
            let Some(truncated) = crate::truncate_to_decimal_places(available.inner(), 0)? else {
                return Ok(CounterTradePreflight::Skipped(
                    CounterTradeSkipReason::InsufficientEquity {
                        required: order.shares,
                        available,
                    },
                ));
            };
            (
                FractionalShares::new(truncated),
                crate::hedge_floor::whole_share_floor(floor)?,
            )
        };
        Ok(crate::resolve_sell_preflight(
            order,
            tradable_available,
            floor,
        )?)
    }

    async fn preflight_buy_cash(
        &self,
        order: &MarketOrder,
        limit_price: Option<Positive<Usd>>,
        quantity_decimals: u8,
        reserved: BuyingPowerReservationCents,
    ) -> Result<CounterTradePreflight, AlpacaBrokerApiError> {
        let (reference_price, slippage_bps) = match limit_price {
            Some(price) => (price, 0),
            None => (
                self.inner.fetch_latest_trade_price(&order.symbol).await?,
                self.counter_trade_slippage_bps,
            ),
        };
        let funds = self.inner.account_funds().await?;
        let reserved_cents: i64 = reserved.get().try_into().map_err(|_| {
            AlpacaBrokerApiError::BuyingPowerReservationOutOfRange {
                reserved_cents: reserved.get(),
            }
        })?;
        let available = funds.buying_power.checked_sub(reserved_cents).ok_or(
            AlpacaBrokerApiError::BuyingPowerReservationOverflow {
                available_cents: funds.buying_power,
                reserved_cents,
            },
        )?;
        let preflight = resolve_buy_preflight(
            order,
            reference_price,
            slippage_bps,
            available,
            quantity_decimals,
        )?;
        if let CounterTradePreflight::Allowed { .. } = preflight {
            debug!(target: "broker", symbol = %order.symbol, "Preflight passed: sufficient buying power for buy");
        }
        Ok(preflight)
    }

    pub async fn withdrawable_cash_cents(&self) -> Result<Option<i64>, AlpacaBrokerApiError> {
        Ok(self.inner.withdrawable_cash_cents().await?)
    }

    pub async fn convert_usdc_usd(
        &self,
        conversion: shared::ConversionOrder,
        id: &ClientOrderId,
    ) -> Result<shared::CryptoOrderResponse, AlpacaBrokerApiError> {
        Ok(self
            .inner
            .convert_usdc_usd(conversion, &shared_client_id(id))
            .await?)
    }

    pub async fn find_conversion_order(
        &self,
        id: &ClientOrderId,
    ) -> Result<Option<shared::CryptoOrderResponse>, AlpacaBrokerApiError> {
        Ok(self
            .inner
            .find_conversion_order(&shared_client_id(id))
            .await?)
    }

    pub async fn poll_conversion_to_terminal(
        &self,
        order_id: Uuid,
    ) -> Result<shared::CryptoOrderResponse, AlpacaBrokerApiError> {
        Ok(self.inner.poll_conversion_to_terminal(order_id).await?)
    }

    pub async fn create_journal(
        &self,
        destination: AlpacaAccountId,
        symbol: &Symbol,
        quantity: Positive<FractionalShares>,
    ) -> Result<shared::JournalResponse, AlpacaBrokerApiError> {
        Ok(self
            .inner
            .create_journal(destination, symbol, quantity)
            .await?)
    }

    pub async fn place_alpaca_limit_order(
        &self,
        order: AlpacaLimitOrder,
    ) -> Result<OrderPlacement<String>, AlpacaBrokerApiError> {
        let shared_order = shared::AlpacaLimitOrder {
            symbol: order.symbol,
            shares: order.shares,
            direction: shared_direction(order.direction),
            limit_price: order.limit_price,
            extended_hours: order.extended_hours,
            client_order_id: shared_client_id(&order.client_order_id),
        };
        Ok(order_placement(
            self.inner.place_alpaca_limit_order(shared_order).await?,
        ))
    }

    pub async fn get_asset_details(
        &self,
        symbol: &Symbol,
    ) -> Result<AssetDetails, AlpacaBrokerApiError> {
        let asset = self.inner.get_asset_details(symbol).await?;
        Ok(asset)
    }

    pub async fn fetch_latest_overnight_quote(
        &self,
        symbol: &Symbol,
    ) -> Result<IndicativeQuote, AlpacaBrokerApiError> {
        let quote = self.inner.fetch_latest_overnight_quote(symbol).await?;
        Ok(IndicativeQuote {
            quote: latest_quote(quote.quote)?,
            at: quote.at,
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use st0x_float_macro::float;

    use super::*;
    use crate::alpaca_broker_api::{AlpacaBrokerApiMode, AlpacaBrokerAuth, TimeInForce};

    async fn broker(
        cash: rain_math_float::Float,
    ) -> (AlpacaBrokerApi, shared::mock::AlpacaBrokerMock) {
        let mock = shared::mock::AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![(Symbol::new("AAPL").unwrap(), float!(100))])
            .symbol_positions(vec![])
            .initial_cash(cash)
            .call()
            .await;
        let ctx = broker_ctx(&mock);
        (AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap(), mock)
    }

    fn broker_ctx(mock: &shared::mock::AlpacaBrokerMock) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            auth: AlpacaBrokerAuth::Basic {
                api_key: shared::mock::TEST_API_KEY.to_string(),
                api_secret: shared::mock::TEST_API_SECRET.to_string(),
            },
            account_id: AlpacaAccountId::new(shared::mock::TEST_ACCOUNT_ID.parse().unwrap()),
            mode: Some(AlpacaBrokerApiMode::Mock(mock.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: 0,
            hedge_floor: HedgeFloor::default(),
        }
    }

    fn order(direction: Direction) -> MarketOrder {
        MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            direction,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        }
    }

    #[tokio::test]
    async fn shared_order_preserves_local_direction_id_and_recovery() {
        let (broker, _mock) = broker(float!(1000)).await;
        let requested = order(Direction::Buy);
        let placement = broker.place_market_order(requested.clone()).await.unwrap();
        assert_eq!(placement.direction, Direction::Buy);
        assert_eq!(placement.symbol, requested.symbol);
        assert!(
            broker
                .get_order_by_client_order_id(&requested.client_order_id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            broker.recover_order_by_client_id(&requested).await,
            Err(AlpacaBrokerApiError::IncompleteOrder {
                field: shared::MissingOrderField::ExtendedHours,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn order_status_preserves_fill_and_failure_terminality() {
        let (broker, mock) = broker(float!(1000)).await;
        let placed = broker
            .place_market_order(order(Direction::Buy))
            .await
            .unwrap();
        assert!(matches!(
            broker.get_order_status(&placed.order_id).await.unwrap(),
            OrderState::Filled { order_id, .. } if order_id.as_ref() == placed.order_id
        ));

        mock.set_mode(shared::mock::MockMode::OrderRejected);
        let rejected = broker
            .place_market_order(order(Direction::Buy))
            .await
            .unwrap();
        assert!(matches!(
            broker.get_order_status(&rejected.order_id).await.unwrap(),
            OrderState::Failed {
                terminality: OrderFailureTerminality::Terminal,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn cancel_missing_order_preserves_order_not_found() {
        let (broker, _mock) = broker(float!(1000)).await;
        assert_eq!(
            broker
                .cancel_order(&Uuid::new_v4().to_string())
                .await
                .unwrap(),
            CancellationOutcome::OrderNotFound
        );
    }

    #[tokio::test]
    async fn invalid_header_credentials_fail_initialization() {
        let (_broker, mock) = broker(float!(1000)).await;
        let mut ctx = broker_ctx(&mock);
        ctx.auth = AlpacaBrokerAuth::Basic {
            api_key: "invalid\nkey".to_string(),
            api_secret: "secret".to_string(),
        };
        assert!(AlpacaBrokerApi::try_from_ctx(ctx).await.is_err());
    }

    #[tokio::test]
    async fn latest_quote_and_session_status_cross_the_adapter() {
        let (broker, _mock) = broker(float!(1000)).await;
        let quote = broker
            .fetch_latest_quote(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert!(quote.bid().inner().inner().eq(float!(100)).unwrap());
        assert!(quote.ask().inner().inner().eq(float!(100)).unwrap());
        let status = broker.market_session_status().await.unwrap();
        assert_eq!(status.session, MarketSession::Regular);
    }

    #[test]
    fn context_conversion_keeps_mode_and_time_in_force() {
        let account_id = AlpacaAccountId::new(Uuid::new_v4());
        let ctx = AlpacaBrokerApiCtx {
            auth: AlpacaBrokerAuth::Basic {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
            },
            account_id,
            mode: Some(AlpacaBrokerApiMode::Production),
            asset_cache_ttl: std::time::Duration::from_secs(17),
            time_in_force: TimeInForce::MarketOnClose,
            counter_trade_slippage_bps: 10,
            hedge_floor: HedgeFloor::default(),
        };
        let shared = ctx.to_shared();
        assert_eq!(shared.account_id, account_id);
        assert_eq!(shared.mode, Some(shared::AlpacaBrokerApiMode::Production));
        assert_eq!(shared.time_in_force, shared::TimeInForce::MarketOnClose);
        assert_eq!(shared.asset_cache_ttl, std::time::Duration::from_secs(17));
    }

    #[tokio::test]
    async fn context_activity_query_uses_shared_mock_context() {
        let (_broker, mock) = broker(float!(1000)).await;
        let activities = broker_ctx(&mock)
            .fetch_account_activities(&shared::AccountActivitiesQuery::pnl(None, None))
            .await
            .unwrap();
        assert!(activities.is_empty());
    }

    #[test]
    fn nonterminal_failure_stays_nonterminal() {
        let state = order_state(shared::OrderState::Failed {
            failed_at: Utc::now(),
            error_reason: Some("suspended".to_string()),
            shares_filled: None,
            avg_price: None,
            terminality: shared::OrderFailureTerminality::NotTerminal,
        });
        assert!(matches!(
            state,
            OrderState::Failed {
                terminality: OrderFailureTerminality::NotTerminal,
                error_reason: Some(reason),
                ..
            } if reason == "suspended"
        ));
    }

    #[test]
    fn session_status_keeps_close_metadata_and_gap() {
        let closes_at = Utc::now();
        let status = session_status(shared::MarketSessionStatus {
            session: shared::MarketSession::Extended,
            session_opens_at: None,
            regular_session_closes_at: None,
            extended_session_closes_at: Some(closes_at),
            post_close_gap: shared::PostCloseGap::MultiDayClosure,
        });
        assert_eq!(status.session, MarketSession::Extended);
        assert_eq!(status.extended_session_closes_at, Some(closes_at));
        assert_eq!(status.post_close_gap, PostCloseGap::MultiDayClosure);
    }

    #[tokio::test]
    async fn preflight_subtracts_reserved_buying_power() {
        let (broker, _mock) = broker(float!(100)).await;
        let preflight = broker
            .preflight_counter_trade_with_reserved_buying_power(
                order(Direction::Buy),
                BuyingPowerReservationCents::new(10_000).unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            preflight,
            CounterTradePreflight::Skipped(CounterTradeSkipReason::InsufficientBuyingPower { .. })
        ));
    }
}
