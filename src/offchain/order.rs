//! OffchainOrder CQRS/ES aggregate for tracking broker order lifecycle
//! (Pending -> Submitted -> Filled/Failed) plus the per-job machinery that
//! drives that lifecycle to a terminal state.
//!
//! # Per-job module layout
//!
//! Each apalis job that operates on an `OffchainOrder` lives in its own file
//! and carries its own `*Ctx` containing only the dependencies that job
//! actually uses. There is no shared umbrella context -- bag-of-everything
//! contexts obscure which job needs which dependency.
//!
//! - [`poll_status`] -- polls the broker for status and routes the result.
//! - [`reconcile_fill`] -- records a successful fill on the aggregate and
//!   position.
//! - [`handle_rejection`] -- records a broker rejection on the aggregate and
//!   position.
//!
//! [`JobError`] is shared across all three jobs because every job converts
//! the same upstream error sources (executor errors, aggregate send failures,
//! queue push failures). Splitting it per-job would duplicate `#[from]`
//! conversions without adding type safety.

pub(crate) mod handle_rejection;
pub(crate) mod poll_status;
pub(crate) mod reconcile_fill;

pub(crate) use handle_rejection::{HandleOrderRejection, HandleOrderRejectionJobQueue};
pub(crate) use poll_status::{
    PollOrderStatus, PollOrderStatusJobQueue, recover_submitted_offchain_orders,
};
pub(crate) use reconcile_fill::{ReconcileOrderFill, ReconcileOrderFillJobQueue};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use metrics::{counter, histogram};
use rain_math_float::FloatError;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, warn};
use uuid::Uuid;

use st0x_dto::{Direction, Trade, TradeOutcome, TradingVenue};
use st0x_event_sorcery::{DomainEvent, EventSourced, SendError, Store, Table};
use st0x_execution::{
    AlpacaBrokerApiError, CancellationOutcome, ClientOrderId, ExecutionError, Executor,
    ExecutorOrderId, FractionalShares, LimitOrder, MarketOrder, MarketSession, OrderState,
    PersistenceError, Positive, SupportedExecutor, Symbol,
};
use st0x_finance::{NonNegative, NotNonNegative, Usd};

use crate::conductor::job::QueuePushError;
use crate::onchain::OnChainError;
use crate::position::{Position, PositionCommand};

/// Errors surfaced by the per-order job pipeline.
///
/// Each concrete executor error gets its own variant via `#[from]` rather
/// than being boxed: the [`Job`](crate::conductor::job::Job) impls bound
/// `JobError: From<E::Error>` so `?` lifts whichever executor error the
/// caller picked. Adding a new executor means adding one variant here.
#[derive(Debug, thiserror::Error)]
pub(crate) enum JobError {
    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),
    #[error("Alpaca broker API error: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Persistence error: {0}")]
    Persistence(#[from] PersistenceError),
    #[error("Onchain error: {0}")]
    OnChain(#[from] OnChainError),
    #[error("Projection query error: {0}")]
    OffchainOrderProjection(#[from] st0x_event_sorcery::ProjectionError<OffchainOrder>),
    #[error("Offchain order aggregate error: {0}")]
    OffchainOrderAggregate(#[from] st0x_event_sorcery::SendError<OffchainOrder>),
    #[error("Position aggregate error: {0}")]
    PositionAggregate(#[from] st0x_event_sorcery::SendError<Position>),
    #[error("Failed to enqueue follow-up job: {0}")]
    Enqueue(#[from] QueuePushError),
    #[error("Offchain order invariant violation: {0}")]
    OffchainOrder(#[from] OffchainOrderError),
}

#[derive(Debug, Clone)]
pub(crate) struct OffchainOrderPlacement {
    symbol: Symbol,
    shares: Positive<FractionalShares>,
    direction: Direction,
    executor: SupportedExecutor,
    client_order_id: ClientOrderId,
    kind: CounterTradeOrderKind,
}

impl OffchainOrderPlacement {
    pub(crate) fn market(
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        client_order_id: ClientOrderId,
    ) -> Self {
        Self::with_kind(
            symbol,
            shares,
            direction,
            executor,
            client_order_id,
            CounterTradeOrderKind::Market,
        )
    }

    pub(crate) fn with_kind(
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        client_order_id: ClientOrderId,
        kind: CounterTradeOrderKind,
    ) -> Self {
        Self {
            symbol,
            shares,
            direction,
            executor,
            client_order_id,
            kind,
        }
    }
}

/// Drives one offchain order placement end to end: records intent via `Place`,
/// performs the external `place_market_order` while the order is still
/// `Pending`, then feeds the broker outcome back as
/// `MarkAccepted`/`MarkPlacementFailed`.
///
/// This is the single broker call site, lifted out of the (now pure) `Place`
/// handler so the side effect lives in the durable placement path rather than a
/// command handler. At-least-once safe: a retry whose order already
/// left `Pending` skips the broker call entirely, and the broker dedupes on
/// `client_order_id` as a second line of defense. Returns the final order state
/// so callers can react (roll the position back on `Failed`, poll on
/// `Submitted`).
///
/// **Concurrency invariant:** a placement-failure outcome is recorded via
/// `MarkPlacementFailed`, which the aggregate honours only while the order is
/// still `Pending`. A stale attempt whose broker call errored after a concurrent
/// attempt already advanced the order past `Pending` is rejected by the handler
/// against the aggregate's authoritative state, so it can never strand a live
/// order. Callers on the live concurrent path (the trade-processing path and
/// `PlaceHedge`) still hold `counter_trade_submission_lock` to serialise
/// placement attempts; startup orphan recovery and the CLI `test-trade` command
/// run without it, which is safe because no concurrent placement runs there.
pub(crate) async fn place_offchain_order_at_broker(
    store: &Store<OffchainOrder>,
    order_placer: &dyn OrderPlacer,
    offchain_order_id: &OffchainOrderId,
    placement: OffchainOrderPlacement,
) -> Result<Option<OffchainOrder>, SendError<OffchainOrder>> {
    let OffchainOrderPlacement {
        symbol,
        shares,
        direction,
        executor,
        client_order_id,
        kind,
    } = placement;

    store
        .send(
            offchain_order_id,
            OffchainOrderCommand::Place {
                symbol: symbol.clone(),
                shares,
                direction,
                executor,
                client_order_id: client_order_id.clone(),
                kind: kind.clone(),
            },
        )
        .await?;

    // Only call the broker while the order is still Pending. A retry whose
    // outcome already landed (Submitted, or a terminal state) must not place a
    // second time. An exhaustive match forces a conscious decision for any
    // future state rather than letting it silently skip placement.
    let placed = store.load(offchain_order_id).await?;
    match placed {
        Some(OffchainOrder::Pending { .. }) => {}
        settled @ (Some(
            OffchainOrder::Submitted { .. }
            | OffchainOrder::PartiallyFilled { .. }
            | OffchainOrder::Cancelling { .. }
            | OffchainOrder::Filled { .. }
            | OffchainOrder::Failed { .. }
            | OffchainOrder::Cancelled { .. },
        )
        | None) => return Ok(settled),
    }

    // Capture metric labels before `symbol` is moved into the market order.
    let symbol_label = symbol.to_string();
    let direction_label = match direction {
        Direction::Buy => "buy",
        Direction::Sell => "sell",
    };

    let placement = match kind {
        CounterTradeOrderKind::Market => {
            let market_order = MarketOrder {
                symbol,
                shares,
                direction,
                client_order_id,
            };
            order_placer.place_market_order(market_order).await
        }
        CounterTradeOrderKind::ExtendedHoursLimit { limit_price } => {
            let limit_order = LimitOrder {
                symbol,
                shares,
                direction,
                limit_price,
                extended_hours: true,
                client_order_id,
            };
            order_placer.place_limit_order(limit_order).await
        }
    };
    let outcome = match placement {
        Ok(result) => {
            counter!(
                "hedge_trades_total",
                "symbol" => symbol_label,
                "direction" => direction_label
            )
            .increment(1);

            if result.placed_shares > shares {
                // A broker over-fill is real shares we now hold; record it as an
                // acceptance (ADR 0009) so the order keeps its executor_order_id
                // and is polled to a terminal state, rather than failed and
                // re-driven into an unbounded loop around a live, unpolled order.
                // The excess is reconciled as ordinary net exposure by the
                // periodic CheckPositions scan.
                warn!(
                    %offchain_order_id,
                    placed = %result.placed_shares,
                    requested = %shares,
                    "Broker placed more shares than requested; recording the over-fill as accepted"
                );
            }

            OffchainOrderCommand::MarkAccepted {
                executor_order_id: result.executor_order_id,
                placed_shares: result.placed_shares,
                submitted_at: Utc::now(),
                market_session: market_session_from_extended(result.is_extended_hours),
                limit_price: result.limit_price,
            }
        }
        Err(error) => {
            counter!(
                "broker_errors_total",
                "symbol" => symbol_label,
                "kind" => "place_order_failed"
            )
            .increment(1);

            OffchainOrderCommand::MarkPlacementFailed {
                error: error.to_string(),
            }
        }
    };

    store.send(offchain_order_id, outcome).await?;
    store.load(offchain_order_id).await
}

/// Derives the broker-side [`ClientOrderId`] for a placement attempt.
///
/// When a prior attempt failed after the broker accepted the order, the
/// position aggregate stashes that attempt's [`OffchainOrderId`] as the
/// idempotency anchor. Retries must reuse its UUID as `client_order_id` so the
/// broker dedupes rather than double-submitting. Lives next to
/// [`place_offchain_order_at_broker`] so every placement path -- the
/// trade-processing path, the hedge job, the CLI, and startup orphan recovery --
/// derives the key the same way.
pub(crate) fn client_order_id_for_placement(
    offchain_order_id: OffchainOrderId,
    last_failed_offchain_order_id: Option<OffchainOrderId>,
) -> ClientOrderId {
    let idempotency_source = last_failed_offchain_order_id.unwrap_or(offchain_order_id);
    ClientOrderId::from_uuid(idempotency_source.as_uuid())
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RetainedFill {
    Priced {
        shares_filled: FractionalShares,
        avg_price: Usd,
        partially_filled_at: DateTime<Utc>,
    },
    Unpriced {
        shares_filled: FractionalShares,
    },
}

impl RetainedFill {
    fn priced(
        shares_filled: FractionalShares,
        avg_price: Usd,
        partially_filled_at: DateTime<Utc>,
    ) -> Self {
        Self::Priced {
            shares_filled,
            avg_price,
            partially_filled_at,
        }
    }

    fn shares_filled(self) -> FractionalShares {
        match self {
            Self::Priced { shares_filled, .. } | Self::Unpriced { shares_filled } => shares_filled,
        }
    }
}

fn regular_market_session() -> MarketSession {
    MarketSession::Regular
}

fn deserialize_market_session<'de, D>(deserializer: D) -> Result<MarketSession, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum EncodedMarketSession {
        Current(MarketSession),
        LegacyExtendedHours(bool),
    }

    match EncodedMarketSession::deserialize(deserializer)? {
        EncodedMarketSession::Current(session) => Ok(session),
        EncodedMarketSession::LegacyExtendedHours(is_extended_hours) => {
            Ok(market_session_from_extended(is_extended_hours))
        }
    }
}

fn market_session_from_extended(is_extended_hours: bool) -> MarketSession {
    if is_extended_hours {
        MarketSession::Extended
    } else {
        MarketSession::Regular
    }
}

fn placed_event(
    symbol: Symbol,
    shares: Positive<FractionalShares>,
    direction: Direction,
    executor: SupportedExecutor,
    client_order_id: &ClientOrderId,
    kind: &CounterTradeOrderKind,
    placed_at: DateTime<Utc>,
) -> OffchainOrderEvent {
    let requested_market_session = kind.market_session();
    let limit_price = match kind {
        CounterTradeOrderKind::ExtendedHoursLimit { limit_price } => Some(*limit_price),
        CounterTradeOrderKind::Market => None,
    };

    OffchainOrderEvent::Placed {
        symbol,
        shares,
        direction,
        executor,
        placed_at,
        is_extended_hours: requested_market_session == MarketSession::Extended,
        limit_price,
        client_order_id: Some(client_order_id.clone()),
    }
}

fn validate_place_replay(
    existing: &OffchainOrder,
    symbol: &Symbol,
    direction: Direction,
    executor: SupportedExecutor,
) -> Result<Vec<OffchainOrderEvent>, OffchainOrderError> {
    if symbol != existing.symbol()
        || direction != existing.direction()
        || executor != existing.executor()
    {
        return Err(OffchainOrderError::PlacePayloadMismatch);
    }

    Ok(vec![])
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OffchainOrder {
    Pending {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        placed_at: DateTime<Utc>,
        #[serde(
            default = "regular_market_session",
            alias = "is_extended_hours",
            deserialize_with = "deserialize_market_session"
        )]
        market_session: MarketSession,
    },
    /// `shares` carries the broker-accepted quantity for orders placed after the
    /// durable-job extraction (built from [`OffchainOrderEvent::Accepted`]'s
    /// `placed_shares`), but the originally-requested quantity for pre-extraction
    /// orders that replay the legacy [`OffchainOrderEvent::Submitted`] event. The
    /// two can differ when the broker truncated to its precision; consumers that
    /// compare fills against `shares` should treat the legacy value as the
    /// request, not a guaranteed broker-accepted amount.
    Submitted {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        #[serde(default)]
        requested_shares: Option<Positive<FractionalShares>>,
        direction: Direction,
        executor: SupportedExecutor,
        executor_order_id: ExecutorOrderId,
        placed_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
        #[serde(
            default = "regular_market_session",
            alias = "is_extended_hours",
            deserialize_with = "deserialize_market_session"
        )]
        market_session: MarketSession,
    },
    PartiallyFilled {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        #[serde(default)]
        requested_shares: Option<Positive<FractionalShares>>,
        shares_filled: FractionalShares,
        direction: Direction,
        executor: SupportedExecutor,
        executor_order_id: ExecutorOrderId,
        avg_price: Usd,
        placed_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
        partially_filled_at: DateTime<Utc>,
        #[serde(
            default = "regular_market_session",
            alias = "is_extended_hours",
            deserialize_with = "deserialize_market_session"
        )]
        market_session: MarketSession,
    },
    Cancelling {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        #[serde(default)]
        requested_shares: Option<Positive<FractionalShares>>,
        #[serde(default)]
        retained_fill: Option<RetainedFill>,
        direction: Direction,
        executor: SupportedExecutor,
        executor_order_id: ExecutorOrderId,
        reason: CancellationReason,
        placed_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
        cancel_requested_at: DateTime<Utc>,
        #[serde(
            default = "regular_market_session",
            alias = "is_extended_hours",
            deserialize_with = "deserialize_market_session"
        )]
        market_session: MarketSession,
    },
    Filled {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        executor_order_id: ExecutorOrderId,
        price: Usd,
        placed_at: DateTime<Utc>,
        submitted_at: DateTime<Utc>,
        filled_at: DateTime<Utc>,
    },
    Failed {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        #[serde(default)]
        requested_shares: Option<Positive<FractionalShares>>,
        direction: Direction,
        executor: SupportedExecutor,
        #[serde(default)]
        retained_fill: Option<RetainedFill>,
        #[serde(default)]
        filled_shares: Option<FractionalShares>,
        #[serde(default)]
        executor_order_id: Option<ExecutorOrderId>,
        error: String,
        placed_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
    /// Terminal state after a successful broker cancellation. Distinct
    /// from `Failed` so analytics and the cancel-and-replace recovery
    /// path can tell intentional cancellation apart from broker rejection.
    ///
    /// `retained_fill`/`executor_order_id` carry any priced partial fills the
    /// order incurred before cancellation so the position-side cleanup can
    /// issue `CompleteOffChainOrder`. `requested_shares` and `filled_shares`
    /// separately retain dashboard provenance, including an explicit
    /// broker-reported zero fill.
    Cancelled {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        #[serde(default)]
        requested_shares: Option<Positive<FractionalShares>>,
        #[serde(default)]
        retained_fill: Option<RetainedFill>,
        #[serde(default)]
        filled_shares: Option<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        executor_order_id: ExecutorOrderId,
        reason: CancellationReason,
        placed_at: DateTime<Utc>,
        cancelled_at: DateTime<Utc>,
    },
}

fn originate_offchain_order(event: &OffchainOrderEvent) -> Option<OffchainOrder> {
    use OffchainOrderEvent::Placed;
    match event {
        Placed {
            symbol,
            shares,
            direction,
            executor,
            placed_at,
            is_extended_hours,
            limit_price: _,
            client_order_id: _,
        } => Some(OffchainOrder::Pending {
            symbol: symbol.clone(),
            shares: *shares,
            direction: *direction,
            executor: *executor,
            placed_at: *placed_at,
            market_session: market_session_from_extended(*is_extended_hours),
        }),
        _ => None,
    }
}

async fn cancel_order_events(
    entity: &OffchainOrder,
    services: &dyn OrderPlacer,
    reason: CancellationReason,
) -> Result<Vec<OffchainOrderEvent>, OffchainOrderError> {
    match entity {
        OffchainOrder::Submitted {
            executor_order_id, ..
        }
        | OffchainOrder::PartiallyFilled {
            executor_order_id, ..
        } => {
            let mut events = Vec::new();
            let local_filled = match entity {
                OffchainOrder::PartiallyFilled { shares_filled, .. } => Some(*shares_filled),
                _ => None,
            };
            let pre_cancel_events =
                reconcile_pre_cancel(services, executor_order_id, local_filled, reason).await?;
            let cancel_short_circuit = pre_cancel_events.iter().any(|event| {
                matches!(
                    event,
                    OffchainOrderEvent::Filled { .. }
                        | OffchainOrderEvent::Failed { .. }
                        | OffchainOrderEvent::Cancelled { .. }
                )
            });
            events.extend(pre_cancel_events);

            if cancel_short_circuit {
                return Ok(events);
            }

            match services
                .cancel_order(executor_order_id)
                .await
                .map_err(|error| {
                    tracing::warn!(
                        %executor_order_id,
                        %error,
                        "Failed to cancel order via broker; will retry"
                    );
                    OffchainOrderError::CancelFailed {
                        executor_order_id: executor_order_id.clone(),
                    }
                })? {
                CancellationOutcome::Requested => {
                    events.push(OffchainOrderEvent::CancelRequested {
                        reason,
                        cancel_requested_at: Utc::now(),
                    });
                }
                CancellationOutcome::OrderNotFound => {
                    tracing::warn!(
                        %executor_order_id,
                        ?reason,
                        "Broker no longer recognises order on cancel; resolving as terminally cancelled"
                    );
                    events.push(OffchainOrderEvent::Cancelled {
                        reason,
                        filled_shares: None,
                        cancelled_at: Utc::now(),
                    });
                }
            }

            if reason == CancellationReason::MarketOpenReplacement {
                tracing::info!(
                    target: "hedge",
                    symbol = %entity.symbol(),
                    %executor_order_id,
                    "Regular hours: cancelling extended-hours limit order for market-order replacement"
                );
            }

            Ok(events)
        }
        OffchainOrder::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
        OffchainOrder::Cancelling { .. } => Ok(Vec::new()),
        OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => Err(OffchainOrderError::AlreadyCompleted),
    }
}

/// Why an [`OffchainOrder`] was cancelled. Carried on
/// [`OffchainOrderEvent::Cancelled`] so it can be persisted, projected,
/// and pattern-matched without parsing strings.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CancellationReason {
    /// Extended-hours limit order cancelled at the Extended -> Regular
    /// transition so the next monitor scan can place a market order
    /// instead.
    MarketOpenReplacement,
    /// The broker reported the order cancelled without a locally persisted
    /// cancel request: either an operator/broker-side cancellation (e.g. a
    /// manual Alpaca-dashboard cancel) or a crash that lost the
    /// `CancelRequested` event. Recorded by the poll loop's recovery path,
    /// which cannot distinguish the two -- what it knows is that no local
    /// request reached the event store.
    Unrequested,
}

#[async_trait]
impl EventSourced for OffchainOrder {
    type Id = OffchainOrderId;
    type Event = OffchainOrderEvent;
    type Command = OffchainOrderCommand;
    type Error = OffchainOrderError;
    type Services = Arc<dyn OrderPlacer>;
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "OffchainOrder";
    const PROJECTION: Table = Table("offchain_order_view");
    const SCHEMA_VERSION: u64 = 4;

    fn originate(event: &Self::Event) -> Option<Self> {
        originate_offchain_order(event)
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use OffchainOrderEvent::*;
        match event {
            Placed { .. } => Ok(None),

            Submitted {
                executor_order_id,
                submitted_at,
            } => {
                let Self::Pending {
                    symbol,
                    shares,
                    direction,
                    executor,
                    placed_at,
                    market_session,
                } = entity
                else {
                    return Ok(None);
                };

                Ok(Some(Self::Submitted {
                    symbol: symbol.clone(),
                    shares: *shares,
                    requested_shares: None,
                    direction: *direction,
                    executor: *executor,
                    executor_order_id: executor_order_id.clone(),
                    placed_at: *placed_at,
                    submitted_at: *submitted_at,
                    market_session: *market_session,
                }))
            }

            Accepted {
                executor_order_id,
                placed_shares,
                submitted_at,
                market_session,
                limit_price: _,
            } => {
                let Self::Pending {
                    symbol,
                    shares: requested_shares,
                    direction,
                    executor,
                    placed_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                // The broker-accepted quantity supersedes the requested quantity
                // the pure `Placed` event recorded, so the order carries the
                // amount actually working at the broker from here on.
                Ok(Some(Self::Submitted {
                    symbol: symbol.clone(),
                    shares: *placed_shares,
                    requested_shares: Some(*requested_shares),
                    direction: *direction,
                    executor: *executor,
                    executor_order_id: executor_order_id.clone(),
                    placed_at: *placed_at,
                    submitted_at: *submitted_at,
                    market_session: *market_session,
                }))
            }

            PartiallyFilled {
                shares_filled,
                avg_price,
                partially_filled_at,
            } => Ok(evolve_partially_filled(
                entity,
                *shares_filled,
                *avg_price,
                *partially_filled_at,
            )),

            Filled { price, filled_at } => Ok(evolve_filled(entity, *price, *filled_at)),

            CancelRequested {
                reason,
                cancel_requested_at,
            } => Ok(evolve_cancel_requested(
                entity,
                *reason,
                *cancel_requested_at,
            )),

            Failed {
                error,
                filled_shares,
                failed_at,
            } => Ok(evolve_failed(
                entity,
                error.clone(),
                *filled_shares,
                *failed_at,
            )),

            Cancelled {
                reason,
                filled_shares,
                cancelled_at,
            } => Ok(evolve_cancelled(
                entity,
                *reason,
                *filled_shares,
                *cancelled_at,
            )),
        }
    }

    async fn initialize(
        command: Self::Command,
        _: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use OffchainOrderCommand::*;
        match command {
            // Pure intent: record that an order was requested and enter
            // `Pending`. The broker `place_market_order` call no longer happens
            // here -- the durable placement path performs it and feeds the
            // outcome back via `MarkAccepted`/`MarkFailed`.
            Place {
                symbol,
                shares,
                direction,
                executor,
                client_order_id,
                kind,
            } => Ok(vec![placed_event(
                symbol,
                shares,
                direction,
                executor,
                &client_order_id,
                &kind,
                Utc::now(),
            )]),

            #[cfg(any(test, feature = "test-support"))]
            PlaceAt {
                symbol,
                shares,
                direction,
                executor,
                client_order_id,
                kind,
                placed_at,
            } => Ok(vec![placed_event(
                symbol,
                shares,
                direction,
                executor,
                &client_order_id,
                &kind,
                placed_at,
            )]),

            _ => Err(OffchainOrderError::NotPlaced),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            // Idempotent against a placement retry: the durable path re-sends
            // `Place` for an order it already created, so an exact replay records
            // nothing. A payload that diverges from the existing order is a caller
            // bug, not a replay, and must fail fast rather than silently no-op
            // (financial integrity). `shares` is excluded because a broker
            // over-fill (ADR 0009) legitimately changes the recorded quantity from
            // the originally requested amount.
            OffchainOrderCommand::Place {
                symbol,
                direction,
                executor,
                shares: _,
                client_order_id: _,
                kind: _,
            } => validate_place_replay(self, &symbol, direction, executor),

            #[cfg(any(test, feature = "test-support"))]
            OffchainOrderCommand::PlaceAt {
                symbol,
                direction,
                executor,
                shares: _,
                client_order_id: _,
                kind: _,
                placed_at: _,
            } => validate_place_replay(self, &symbol, direction, executor),

            OffchainOrderCommand::CancelOrder { reason } => {
                cancel_order_events(self, services.as_ref(), reason).await
            }

            OffchainOrderCommand::ConfirmCancellation {
                filled_shares,
                cancelled_at,
            } => confirm_cancellation_events(self, filled_shares, cancelled_at),

            OffchainOrderCommand::MarkUnrequestedCancellation {
                filled_shares,
                cancelled_at,
            } => mark_unrequested_cancellation_events(self, filled_shares, cancelled_at),

            OffchainOrderCommand::UpdatePartialFill {
                shares_filled,
                avg_price,
                partially_filled_at,
            } => match self {
                Self::Submitted { .. } => Ok(vec![OffchainOrderEvent::PartiallyFilled {
                    shares_filled,
                    avg_price,
                    partially_filled_at,
                }]),
                Self::PartiallyFilled {
                    shares_filled: local_filled,
                    ..
                } => {
                    // Cumulative fills must never regress; the guard is
                    // shared by the live and cancelling states so the
                    // monotonicity invariant cannot drift between them.
                    if !broker_fill_exceeds_local(shares_filled, *local_filled)? {
                        tracing::debug!(
                            local_shares_filled = %local_filled,
                            broker_shares_filled = %shares_filled,
                            "Skipping stale or duplicate partial-fill update"
                        );
                        return Ok(Vec::new());
                    }

                    Ok(vec![OffchainOrderEvent::PartiallyFilled {
                        shares_filled,
                        avg_price,
                        partially_filled_at,
                    }])
                }
                Self::Cancelling { retained_fill, .. } => {
                    let local_filled = retained_fill
                        .map(RetainedFill::shares_filled)
                        .unwrap_or(FractionalShares::ZERO);
                    if !broker_fill_exceeds_local(shares_filled, local_filled)? {
                        tracing::debug!(
                            local_shares_filled = %local_filled,
                            broker_shares_filled = %shares_filled,
                            "Skipping stale or duplicate partial-fill update"
                        );
                        return Ok(Vec::new());
                    }

                    Ok(vec![OffchainOrderEvent::PartiallyFilled {
                        shares_filled,
                        avg_price,
                        partially_filled_at,
                    }])
                }
                Self::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
                Self::Filled { .. } | Self::Failed { .. } | Self::Cancelled { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            OffchainOrderCommand::CompleteFill { price, filled_at } => match self {
                Self::Submitted {
                    symbol, placed_at, ..
                }
                | Self::PartiallyFilled {
                    symbol, placed_at, ..
                }
                | Self::Cancelling {
                    symbol, placed_at, ..
                } => {
                    // Wall-clock placement-to-fill latency. A negative delta can
                    // only come from clock skew (never a real latency), so
                    // `to_std()` rejects it and the sample is skipped rather than
                    // recorded as a bogus value -- logged so the skew is not
                    // silently swallowed.
                    match (filled_at - *placed_at).to_std() {
                        Ok(latency) => {
                            histogram!("hedge_fill_latency_seconds", "symbol" => symbol.to_string())
                                .record(latency.as_secs_f64());
                        }
                        Err(error) => {
                            debug!(
                                %symbol, %error,
                                "Skipping hedge_fill_latency sample: fill precedes placement (clock skew)"
                            );
                        }
                    }

                    Ok(vec![OffchainOrderEvent::Filled { price, filled_at }])
                }
                Self::Pending { .. } => Err(OffchainOrderError::NotSubmitted),
                Self::Filled { .. } | Self::Failed { .. } | Self::Cancelled { .. } => {
                    Err(OffchainOrderError::AlreadyCompleted)
                }
            },

            OffchainOrderCommand::MarkAccepted {
                executor_order_id,
                placed_shares,
                submitted_at,
                market_session,
                limit_price,
            } => match self {
                Self::Pending { .. } => Ok(vec![OffchainOrderEvent::Accepted {
                    executor_order_id,
                    placed_shares,
                    submitted_at,
                    market_session,
                    limit_price,
                }]),
                // Idempotent: a retried placement whose order already left
                // `Pending` (acceptance recorded, or already terminal) is a no-op.
                Self::Submitted { .. }
                | Self::PartiallyFilled { .. }
                | Self::Cancelling { .. }
                | Self::Filled { .. }
                | Self::Failed { .. }
                | Self::Cancelled { .. } => Ok(vec![]),
            },

            // Placement-initiated failure. Unlike `MarkFailed` (the poll-rejection
            // path, which may fail a live order), this is honoured only while the
            // order is still `Pending`: a stale placement attempt whose broker call
            // errored must never clobber a `Submitted`/`PartiallyFilled` order a
            // concurrent attempt already accepted. Enforcing it here -- against the
            // aggregate's authoritative state -- closes the load-then-send race a
            // caller-side re-check could not.
            OffchainOrderCommand::MarkPlacementFailed { error } => match self {
                Self::Pending { .. } => Ok(vec![OffchainOrderEvent::Failed {
                    error,
                    filled_shares: None,
                    failed_at: Utc::now(),
                }]),
                Self::Submitted { symbol, .. }
                | Self::PartiallyFilled { symbol, .. }
                | Self::Cancelling { symbol, .. }
                | Self::Filled { symbol, .. }
                | Self::Failed { symbol, .. }
                | Self::Cancelled { symbol, .. } => {
                    warn!(
                        %symbol,
                        "Skipping placement-initiated failure: the order is no longer Pending \
                         (a concurrent placement attempt advanced it); leaving it untouched"
                    );
                    Ok(vec![])
                }
            },

            OffchainOrderCommand::MarkFailed {
                error,
                filled_shares,
                failed_at,
            } => mark_failed_events(self, error, filled_shares, failed_at),
        }
    }
}

fn confirm_cancellation_events(
    order: &OffchainOrder,
    filled_shares: FractionalShares,
    cancelled_at: DateTime<Utc>,
) -> Result<Vec<OffchainOrderEvent>, OffchainOrderError> {
    match order {
        OffchainOrder::Cancelling {
            reason,
            retained_fill,
            ..
        } => {
            validate_terminal_fill_comparison(*retained_fill, Some(filled_shares))?;
            Ok(vec![OffchainOrderEvent::Cancelled {
                reason: *reason,
                filled_shares: Some(filled_shares),
                cancelled_at,
            }])
        }
        OffchainOrder::Pending { .. }
        | OffchainOrder::Submitted { .. }
        | OffchainOrder::PartiallyFilled { .. } => {
            Err(OffchainOrderError::CancellationNotRequested)
        }
        OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => Err(OffchainOrderError::AlreadyCompleted),
    }
}

fn mark_unrequested_cancellation_events(
    order: &OffchainOrder,
    filled_shares: FractionalShares,
    cancelled_at: DateTime<Utc>,
) -> Result<Vec<OffchainOrderEvent>, OffchainOrderError> {
    let retained_fill = match order {
        OffchainOrder::Submitted { .. } => None,
        OffchainOrder::PartiallyFilled {
            shares_filled,
            avg_price,
            partially_filled_at,
            ..
        } => Some(RetainedFill::priced(
            *shares_filled,
            *avg_price,
            *partially_filled_at,
        )),
        OffchainOrder::Pending { .. } | OffchainOrder::Cancelling { .. } => {
            return Err(OffchainOrderError::CancellationNotRequested);
        }
        OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => {
            return Err(OffchainOrderError::AlreadyCompleted);
        }
    };

    validate_terminal_fill_comparison(retained_fill, Some(filled_shares))?;
    Ok(vec![OffchainOrderEvent::Cancelled {
        reason: CancellationReason::Unrequested,
        filled_shares: Some(filled_shares),
        cancelled_at,
    }])
}

fn mark_failed_events(
    order: &OffchainOrder,
    error: String,
    filled_shares: Option<FractionalShares>,
    failed_at: DateTime<Utc>,
) -> Result<Vec<OffchainOrderEvent>, OffchainOrderError> {
    let retained_fill = match order {
        OffchainOrder::Pending { .. } | OffchainOrder::Submitted { .. } => None,
        OffchainOrder::PartiallyFilled {
            shares_filled,
            avg_price,
            partially_filled_at,
            ..
        } => Some(RetainedFill::priced(
            *shares_filled,
            *avg_price,
            *partially_filled_at,
        )),
        OffchainOrder::Cancelling { retained_fill, .. } => *retained_fill,
        // Idempotent: re-failing an already-failed order records nothing.
        OffchainOrder::Failed { .. } => return Ok(vec![]),
        OffchainOrder::Filled { .. } | OffchainOrder::Cancelled { .. } => {
            return Err(OffchainOrderError::AlreadyCompleted);
        }
    };

    validate_terminal_fill_comparison(retained_fill, filled_shares)?;
    Ok(vec![OffchainOrderEvent::Failed {
        error,
        filled_shares,
        failed_at,
    }])
}

fn evolve_filled(
    entity: &OffchainOrder,
    price: Usd,
    filled_at: DateTime<Utc>,
) -> Option<OffchainOrder> {
    match entity {
        OffchainOrder::Submitted {
            symbol,
            shares,
            direction,
            executor,
            executor_order_id,
            placed_at,
            submitted_at,
            ..
        }
        | OffchainOrder::PartiallyFilled {
            symbol,
            shares,
            direction,
            executor,
            executor_order_id,
            placed_at,
            submitted_at,
            ..
        }
        | OffchainOrder::Cancelling {
            symbol,
            shares,
            direction,
            executor,
            executor_order_id,
            placed_at,
            submitted_at,
            ..
        } => Some(OffchainOrder::Filled {
            symbol: symbol.clone(),
            shares: *shares,
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            price,
            placed_at: *placed_at,
            submitted_at: *submitted_at,
            filled_at,
        }),

        OffchainOrder::Pending { .. }
        | OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => None,
    }
}

fn evolve_partially_filled(
    entity: &OffchainOrder,
    shares_filled: FractionalShares,
    avg_price: Usd,
    partially_filled_at: DateTime<Utc>,
) -> Option<OffchainOrder> {
    match entity {
        OffchainOrder::Submitted {
            symbol,
            shares,
            requested_shares,
            direction,
            executor,
            executor_order_id,
            placed_at,
            submitted_at,
            market_session,
        }
        | OffchainOrder::PartiallyFilled {
            symbol,
            shares,
            requested_shares,
            direction,
            executor,
            executor_order_id,
            placed_at,
            submitted_at,
            market_session,
            ..
        } => Some(OffchainOrder::PartiallyFilled {
            symbol: symbol.clone(),
            shares: *shares,
            requested_shares: *requested_shares,
            shares_filled,
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            avg_price,
            placed_at: *placed_at,
            submitted_at: *submitted_at,
            partially_filled_at,
            market_session: *market_session,
        }),
        OffchainOrder::Cancelling {
            symbol,
            shares,
            requested_shares,
            direction,
            executor,
            executor_order_id,
            reason,
            placed_at,
            submitted_at,
            cancel_requested_at,
            market_session,
            ..
        } => Some(OffchainOrder::Cancelling {
            symbol: symbol.clone(),
            shares: *shares,
            requested_shares: *requested_shares,
            retained_fill: Some(RetainedFill::priced(
                shares_filled,
                avg_price,
                partially_filled_at,
            )),
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            reason: *reason,
            placed_at: *placed_at,
            submitted_at: *submitted_at,
            cancel_requested_at: *cancel_requested_at,
            market_session: *market_session,
        }),
        OffchainOrder::Pending { .. }
        | OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => None,
    }
}

fn evolve_cancel_requested(
    entity: &OffchainOrder,
    reason: CancellationReason,
    cancel_requested_at: DateTime<Utc>,
) -> Option<OffchainOrder> {
    match entity {
        OffchainOrder::Submitted {
            symbol,
            shares,
            requested_shares,
            direction,
            executor,
            executor_order_id,
            placed_at,
            submitted_at,
            market_session,
        } => Some(OffchainOrder::Cancelling {
            symbol: symbol.clone(),
            shares: *shares,
            requested_shares: *requested_shares,
            retained_fill: None,
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            reason,
            placed_at: *placed_at,
            submitted_at: *submitted_at,
            cancel_requested_at,
            market_session: *market_session,
        }),
        OffchainOrder::PartiallyFilled {
            symbol,
            shares,
            requested_shares,
            shares_filled,
            direction,
            executor,
            executor_order_id,
            avg_price,
            placed_at,
            submitted_at,
            partially_filled_at,
            market_session,
            ..
        } => Some(OffchainOrder::Cancelling {
            symbol: symbol.clone(),
            shares: *shares,
            requested_shares: *requested_shares,
            retained_fill: Some(RetainedFill::priced(
                *shares_filled,
                *avg_price,
                *partially_filled_at,
            )),
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            reason,
            placed_at: *placed_at,
            submitted_at: *submitted_at,
            cancel_requested_at,
            market_session: *market_session,
        }),
        OffchainOrder::Pending { .. }
        | OffchainOrder::Cancelling { .. }
        | OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => None,
    }
}

fn evolve_failed(
    entity: &OffchainOrder,
    error: String,
    filled_shares: Option<FractionalShares>,
    failed_at: DateTime<Utc>,
) -> Option<OffchainOrder> {
    match entity {
        OffchainOrder::Pending {
            symbol,
            shares,
            direction,
            executor,
            placed_at,
            ..
        } => Some(OffchainOrder::Failed {
            symbol: symbol.clone(),
            shares: *shares,
            requested_shares: None,
            direction: *direction,
            executor: *executor,
            retained_fill: retained_fill_for_terminal(None, filled_shares),
            filled_shares,
            executor_order_id: None,
            error,
            placed_at: *placed_at,
            failed_at,
        }),
        OffchainOrder::Submitted {
            symbol,
            shares,
            requested_shares,
            direction,
            executor,
            executor_order_id,
            placed_at,
            ..
        } => Some(OffchainOrder::Failed {
            symbol: symbol.clone(),
            shares: *shares,
            requested_shares: *requested_shares,
            direction: *direction,
            executor: *executor,
            retained_fill: retained_fill_for_terminal(None, filled_shares),
            filled_shares,
            executor_order_id: Some(executor_order_id.clone()),
            error,
            placed_at: *placed_at,
            failed_at,
        }),
        OffchainOrder::PartiallyFilled {
            symbol,
            shares,
            requested_shares,
            shares_filled,
            direction,
            executor,
            executor_order_id,
            avg_price,
            placed_at,
            partially_filled_at,
            ..
        } => Some(OffchainOrder::Failed {
            symbol: symbol.clone(),
            shares: *shares,
            requested_shares: *requested_shares,
            direction: *direction,
            executor: *executor,
            retained_fill: retained_fill_for_terminal(
                Some(RetainedFill::priced(
                    *shares_filled,
                    *avg_price,
                    *partially_filled_at,
                )),
                filled_shares,
            ),
            filled_shares: filled_shares.or(Some(*shares_filled)),
            executor_order_id: Some(executor_order_id.clone()),
            error,
            placed_at: *placed_at,
            failed_at,
        }),
        OffchainOrder::Cancelling {
            symbol,
            shares,
            requested_shares,
            retained_fill,
            direction,
            executor,
            executor_order_id,
            placed_at,
            ..
        } => Some(OffchainOrder::Failed {
            symbol: symbol.clone(),
            shares: *shares,
            requested_shares: *requested_shares,
            direction: *direction,
            executor: *executor,
            retained_fill: retained_fill_for_terminal(*retained_fill, filled_shares),
            filled_shares: filled_shares.or_else(|| retained_fill.map(RetainedFill::shares_filled)),
            executor_order_id: Some(executor_order_id.clone()),
            error,
            placed_at: *placed_at,
            failed_at,
        }),
        OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => None,
    }
}

fn evolve_cancelled(
    entity: &OffchainOrder,
    reason: CancellationReason,
    filled_shares: Option<FractionalShares>,
    cancelled_at: DateTime<Utc>,
) -> Option<OffchainOrder> {
    match entity {
        OffchainOrder::Submitted {
            symbol,
            shares,
            requested_shares,
            direction,
            executor,
            executor_order_id,
            placed_at,
            ..
        } => Some(OffchainOrder::Cancelled {
            symbol: symbol.clone(),
            shares: *shares,
            requested_shares: *requested_shares,
            retained_fill: retained_fill_for_terminal(None, filled_shares),
            filled_shares,
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            reason,
            placed_at: *placed_at,
            cancelled_at,
        }),
        OffchainOrder::PartiallyFilled {
            symbol,
            shares,
            requested_shares,
            shares_filled,
            direction,
            executor,
            executor_order_id,
            avg_price,
            placed_at,
            partially_filled_at,
            ..
        } => Some(OffchainOrder::Cancelled {
            symbol: symbol.clone(),
            shares: *shares,
            requested_shares: *requested_shares,
            retained_fill: retained_fill_for_terminal(
                Some(RetainedFill::priced(
                    *shares_filled,
                    *avg_price,
                    *partially_filled_at,
                )),
                filled_shares,
            ),
            filled_shares: filled_shares.or(Some(*shares_filled)),
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            reason,
            placed_at: *placed_at,
            cancelled_at,
        }),
        OffchainOrder::Cancelling {
            symbol,
            shares,
            requested_shares,
            retained_fill,
            direction,
            executor,
            executor_order_id,
            reason: requested_reason,
            placed_at,
            ..
        } => Some(OffchainOrder::Cancelled {
            symbol: symbol.clone(),
            shares: *shares,
            requested_shares: *requested_shares,
            retained_fill: retained_fill_for_terminal(*retained_fill, filled_shares),
            filled_shares: filled_shares.or_else(|| retained_fill.map(RetainedFill::shares_filled)),
            direction: *direction,
            executor: *executor,
            executor_order_id: executor_order_id.clone(),
            reason: *requested_reason,
            placed_at: *placed_at,
            cancelled_at,
        }),
        OffchainOrder::Pending { .. }
        | OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => None,
    }
}

fn retained_fill_for_terminal(
    retained_fill: Option<RetainedFill>,
    terminal_filled_shares: Option<FractionalShares>,
) -> Option<RetainedFill> {
    match terminal_filled_shares {
        Some(shares_filled) if Positive::new(shares_filled).is_ok() => {
            let Some(retained) = retained_fill else {
                return Some(RetainedFill::Unpriced { shares_filled });
            };
            match shares_filled.inner().gt(retained.shares_filled().inner()) {
                Ok(true) => Some(RetainedFill::Unpriced { shares_filled }),
                Ok(false) => Some(retained),
                Err(error) => {
                    tracing::error!(
                        ?error,
                        terminal_shares_filled = %shares_filled,
                        retained_shares_filled = %retained.shares_filled(),
                        "Float comparison failed while replaying a terminal fill; preserving retained evidence"
                    );
                    Some(retained)
                }
            }
        }
        Some(_) | None => retained_fill,
    }
}

fn validate_terminal_fill_comparison(
    retained_fill: Option<RetainedFill>,
    terminal_filled_shares: Option<FractionalShares>,
) -> Result<(), OffchainOrderError> {
    let (Some(retained), Some(terminal)) = (retained_fill, terminal_filled_shares) else {
        return Ok(());
    };
    if Positive::new(terminal).is_err() {
        return Ok(());
    }

    broker_fill_exceeds_local(terminal, retained.shares_filled())?;
    Ok(())
}

struct TerminalQuantityProvenance {
    requested: Positive<FractionalShares>,
    accepted: Option<Positive<FractionalShares>>,
    filled: Option<NonNegative<FractionalShares>>,
    remaining: Option<NonNegative<FractionalShares>>,
    excess: Option<NonNegative<FractionalShares>>,
}

fn terminal_quantity_provenance(
    accepted_or_legacy_requested: Positive<FractionalShares>,
    requested_shares: Option<Positive<FractionalShares>>,
    retained_fill: Option<RetainedFill>,
    terminal_filled_shares: Option<FractionalShares>,
) -> Result<TerminalQuantityProvenance, TradeConversionError> {
    // Modern acceptance preserves the original request separately, proving
    // that the state's `shares` is broker-accepted. Legacy Submitted events do
    // not, so acceptance remains unknown and `shares` is only safe to report as
    // the original request.
    let accepted_shares = requested_shares.map(|_| accepted_or_legacy_requested);
    let requested_shares = requested_shares.unwrap_or(accepted_or_legacy_requested);
    let terminal_fill = terminal_filled_shares.map(NonNegative::new).transpose()?;
    let retained_fill = retained_fill
        .map(RetainedFill::shares_filled)
        .map(NonNegative::new)
        .transpose()?;
    // Cumulative fill evidence cannot regress. If a stale terminal broker
    // response reports less than an earlier persisted partial fill, retain the
    // larger proven quantity rather than replacing it.
    let filled_shares = match (terminal_fill, retained_fill) {
        (Some(terminal), Some(retained))
            if terminal.inner().inner().gt(retained.inner().inner())? =>
        {
            Some(terminal)
        }
        (Some(_), Some(retained)) => Some(retained),
        (Some(terminal), None) => Some(terminal),
        (None, retained) => retained,
    };
    let (remaining_shares, excess_shares) = match (accepted_shares, filled_shares) {
        (Some(accepted), Some(filled)) => {
            let accepted = accepted.inner();
            let filled = filled.inner();
            if filled.inner().gt(accepted.inner())? {
                (
                    Some(NonNegative::new(FractionalShares::ZERO)?),
                    Some(NonNegative::new((filled - accepted)?)?),
                )
            } else {
                (
                    Some(NonNegative::new((accepted - filled)?)?),
                    Some(NonNegative::new(FractionalShares::ZERO)?),
                )
            }
        }
        _ => (None, None),
    };

    Ok(TerminalQuantityProvenance {
        requested: requested_shares,
        accepted: accepted_shares,
        filled: filled_shares,
        remaining: remaining_shares,
        excess: excess_shares,
    })
}

impl OffchainOrder {
    /// Renders a terminal fill or failure as a dashboard [`Trade`].
    pub(crate) fn try_into_trade(
        self,
        id: &OffchainOrderId,
    ) -> Result<Trade, TradeConversionError> {
        let (symbol, shares, direction, executor, occurred_at, outcome) = match self {
            Self::Filled {
                symbol,
                shares,
                direction,
                executor,
                filled_at,
                ..
            } => (
                symbol,
                shares,
                direction,
                executor,
                filled_at,
                TradeOutcome::Filled,
            ),
            Self::Failed {
                symbol,
                shares,
                requested_shares,
                direction,
                executor,
                retained_fill,
                filled_shares,
                error,
                failed_at,
                ..
            } => {
                let quantities = terminal_quantity_provenance(
                    shares,
                    requested_shares,
                    retained_fill,
                    filled_shares,
                )?;

                (
                    symbol,
                    quantities.requested,
                    direction,
                    executor,
                    failed_at,
                    TradeOutcome::Failed {
                        error,
                        accepted_shares: quantities.accepted,
                        filled_shares: quantities.filled,
                        remaining_shares: quantities.remaining,
                        excess_shares: quantities.excess,
                    },
                )
            }
            Self::Cancelled {
                symbol,
                shares,
                requested_shares,
                direction,
                executor,
                retained_fill,
                filled_shares,
                cancelled_at,
                ..
            } => {
                let quantities = terminal_quantity_provenance(
                    shares,
                    requested_shares,
                    retained_fill,
                    filled_shares,
                )?;

                (
                    symbol,
                    quantities.requested,
                    direction,
                    executor,
                    cancelled_at,
                    TradeOutcome::Cancelled {
                        accepted_shares: quantities.accepted,
                        filled_shares: quantities.filled,
                        remaining_shares: quantities.remaining,
                        excess_shares: quantities.excess,
                    },
                )
            }
            Self::Pending { .. } => return Err(TradeConversionError::Pending),
            Self::Submitted { .. } => return Err(TradeConversionError::Submitted),
            Self::PartiallyFilled { .. } => return Err(TradeConversionError::PartiallyFilled),
            Self::Cancelling { .. } => return Err(TradeConversionError::Cancelling),
        };

        Ok(Trade {
            id: id.to_string(),
            occurred_at,
            venue: match executor {
                SupportedExecutor::AlpacaBrokerApi => TradingVenue::Alpaca,
                SupportedExecutor::DryRun => TradingVenue::DryRun,
            },
            direction,
            symbol,
            shares,
            outcome,
        })
    }

    pub(crate) fn symbol(&self) -> &Symbol {
        use OffchainOrder::*;
        match self {
            Pending { symbol, .. }
            | Submitted { symbol, .. }
            | PartiallyFilled { symbol, .. }
            | Cancelling { symbol, .. }
            | Filled { symbol, .. }
            | Failed { symbol, .. }
            | Cancelled { symbol, .. } => symbol,
        }
    }

    pub(crate) fn shares(&self) -> Positive<FractionalShares> {
        use OffchainOrder::*;
        match self {
            Pending { shares, .. }
            | Submitted { shares, .. }
            | PartiallyFilled { shares, .. }
            | Cancelling { shares, .. }
            | Filled { shares, .. }
            | Failed { shares, .. }
            | Cancelled { shares, .. } => *shares,
        }
    }

    pub(crate) fn direction(&self) -> Direction {
        use OffchainOrder::*;
        match self {
            Pending { direction, .. }
            | Submitted { direction, .. }
            | PartiallyFilled { direction, .. }
            | Cancelling { direction, .. }
            | Filled { direction, .. }
            | Failed { direction, .. }
            | Cancelled { direction, .. } => *direction,
        }
    }

    pub(crate) fn executor(&self) -> SupportedExecutor {
        use OffchainOrder::*;
        match self {
            Pending { executor, .. }
            | Submitted { executor, .. }
            | PartiallyFilled { executor, .. }
            | Cancelling { executor, .. }
            | Filled { executor, .. }
            | Failed { executor, .. }
            | Cancelled { executor, .. } => *executor,
        }
    }

    pub(crate) fn executor_order_id(&self) -> Option<&ExecutorOrderId> {
        use OffchainOrder::*;
        match self {
            Submitted {
                executor_order_id, ..
            }
            | PartiallyFilled {
                executor_order_id, ..
            }
            | Cancelling {
                executor_order_id, ..
            }
            | Filled {
                executor_order_id, ..
            }
            | Cancelled {
                executor_order_id, ..
            } => Some(executor_order_id),

            // Pending has no broker id yet; Failed's is Option (a failure can
            // occur before the broker assigned one).
            Pending { .. } | Failed { .. } => None,
        }
    }
}

/// How a *terminal* [`OffchainOrder`] should finalize its owning `Position`.
///
/// Shared by the two finalization sites -- the startup orphan-recovery sweep in
/// `conductor` and the cancel-and-replace pass in `position_check` -- so the
/// terminal-state -> position-command mapping lives in one place and cannot
/// drift between them. `broker_timestamp` is the order's own broker event time
/// (`filled_at`/`cancelled_at`/`failed_at`), the moment the broker recorded the
/// outcome -- not the wall-clock time finalization happens to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TerminalPositionFinalization {
    /// Apply the recorded (priced) fill to the position.
    Complete {
        shares_filled: Positive<FractionalShares>,
        direction: Direction,
        executor_order_id: ExecutorOrderId,
        price: Usd,
        broker_timestamp: DateTime<Utc>,
    },
    /// No fill to record -- clear the position's pending reference. The
    /// carried outcome distinguishes intentional cancellation (release the
    /// slot without failure semantics) from broker failure (set the failure
    /// anchor), so callers never re-match the order and the
    /// cancelled-vs-failed mapping cannot drift between them.
    NoFill(NoFillOutcome),
    /// A positive fill quantity with no average price: the fill cannot be
    /// recorded correctly, so the position must NOT be finalized. The order is
    /// terminal and surfaced for manual reconciliation rather than silently
    /// dropping the filled shares.
    UnpricedFill { shares_filled: FractionalShares },
}

/// How a terminal order with no fill ended, mapping 1:1 onto the position
/// command the caller must issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NoFillOutcome {
    /// Broker-confirmed intentional cancellation: issue
    /// `PositionCommand::CancelOffChainOrder`, which clears the pending slot
    /// and the failure/idempotency anchor. `cancelled_at` is the broker's
    /// cancellation time.
    Cancelled {
        reason: CancellationReason,
        cancelled_at: DateTime<Utc>,
    },
    /// Broker rejection/failure: issue `PositionCommand::FailOffChainOrder`,
    /// which sets the failure anchor for the next attempt's idempotency key.
    Failed { error: String },
}

/// Maps a [`TerminalPositionFinalization`] onto the [`PositionCommand`] that
/// finalizes the owning position. Returns `None` for an unpriced fill, which
/// must leave the position pending (callers log and let the every-tick
/// finalize sweep keep surfacing it) rather than dropping the filled shares.
pub(crate) fn position_command_for_finalization(
    finalization: TerminalPositionFinalization,
    offchain_order_id: OffchainOrderId,
) -> Option<PositionCommand> {
    match finalization {
        TerminalPositionFinalization::Complete {
            shares_filled,
            direction,
            executor_order_id,
            price,
            broker_timestamp,
        } => Some(PositionCommand::CompleteOffChainOrder {
            offchain_order_id,
            shares_filled,
            direction,
            executor_order_id,
            price,
            broker_timestamp,
        }),
        TerminalPositionFinalization::NoFill(NoFillOutcome::Cancelled {
            reason,
            cancelled_at,
        }) => Some(PositionCommand::CancelOffChainOrder {
            offchain_order_id,
            reason,
            cancelled_at,
        }),
        TerminalPositionFinalization::NoFill(NoFillOutcome::Failed { error }) => {
            Some(PositionCommand::FailOffChainOrder {
                offchain_order_id,
                error,
            })
        }
        TerminalPositionFinalization::UnpricedFill { .. } => None,
    }
}

pub(crate) async fn finalize_cancelled_position_or_log_unpriced(
    position: &Store<Position>,
    symbol: &Symbol,
    offchain_order_id: OffchainOrderId,
    cancelled: &OffchainOrder,
) -> Result<(), st0x_event_sorcery::SendError<Position>> {
    let command = terminal_position_finalization(cancelled).and_then(|finalization| {
        position_command_for_finalization(finalization, offchain_order_id)
    });

    if let Some(command) = command {
        position.send(symbol, command).await?;
    } else {
        tracing::error!(
            symbol = %symbol,
            %offchain_order_id,
            "Cancelled order after Place retains an unpriced fill; \
             position left pending -- no automated path can finalize \
             this, operator intervention required"
        );
    }

    Ok(())
}

/// Classifies a terminal [`OffchainOrder`] (`Filled`/`Cancelled`/`Failed`) into
/// the [`TerminalPositionFinalization`] it implies. Returns `None` for
/// non-terminal states (the caller leaves the position pending and retries).
pub(crate) fn terminal_position_finalization(
    order: &OffchainOrder,
) -> Option<TerminalPositionFinalization> {
    match order {
        OffchainOrder::Filled {
            shares,
            direction,
            executor_order_id,
            price,
            filled_at,
            ..
        } => Some(TerminalPositionFinalization::Complete {
            shares_filled: *shares,
            direction: *direction,
            executor_order_id: executor_order_id.clone(),
            price: *price,
            broker_timestamp: *filled_at,
        }),

        OffchainOrder::Cancelled {
            retained_fill,
            direction,
            executor_order_id,
            reason,
            cancelled_at,
            ..
        } => Some(classify_terminal_fill(
            *retained_fill,
            *direction,
            executor_order_id.clone(),
            NoFillOutcome::Cancelled {
                reason: *reason,
                cancelled_at: *cancelled_at,
            },
        )),

        OffchainOrder::Failed {
            retained_fill,
            direction,
            executor_order_id: Some(executor_order_id),
            error,
            ..
        } => Some(classify_terminal_fill(
            *retained_fill,
            *direction,
            executor_order_id.clone(),
            NoFillOutcome::Failed {
                error: error.clone(),
            },
        )),

        // Failed with no recorded fill (or no executor id) -- nothing to apply.
        OffchainOrder::Failed { error, .. } => Some(TerminalPositionFinalization::NoFill(
            NoFillOutcome::Failed {
                error: error.clone(),
            },
        )),

        OffchainOrder::Pending { .. }
        | OffchainOrder::Submitted { .. }
        | OffchainOrder::PartiallyFilled { .. }
        | OffchainOrder::Cancelling { .. } => None,
    }
}

/// A priced positive fill -> `Complete`; a positive fill without a price ->
/// `UnpricedFill` (must not be dropped); zero -> `NoFill` carrying the
/// caller-supplied outcome.
fn classify_terminal_fill(
    retained_fill: Option<RetainedFill>,
    direction: Direction,
    executor_order_id: ExecutorOrderId,
    no_fill: NoFillOutcome,
) -> TerminalPositionFinalization {
    match retained_fill {
        Some(RetainedFill::Priced {
            shares_filled,
            avg_price,
            partially_filled_at,
        }) => {
            let Ok(positive) = Positive::new(shares_filled) else {
                return TerminalPositionFinalization::NoFill(no_fill);
            };
            TerminalPositionFinalization::Complete {
                shares_filled: positive,
                direction,
                executor_order_id,
                price: avg_price,
                broker_timestamp: partially_filled_at,
            }
        }
        Some(RetainedFill::Unpriced { shares_filled }) => {
            if Positive::new(shares_filled).is_ok() {
                TerminalPositionFinalization::UnpricedFill { shares_filled }
            } else {
                TerminalPositionFinalization::NoFill(no_fill)
            }
        }
        None => TerminalPositionFinalization::NoFill(no_fill),
    }
}

/// Returns whether the broker-reported cumulative fill strictly exceeds the
/// locally recorded quantity. Cumulative fills must never regress, so callers
/// skip the update when this is `false`.
///
/// A comparison failure is mapped to the structured
/// [`OffchainOrderError::FillComparisonFailed`] (after logging the underlying
/// `FloatError`, which is not serializable) so callers fail closed: the
/// aggregate stays in its prior state and the command retries rather than
/// proceeding with potentially lost fill data.
fn broker_fill_exceeds_local(
    broker_shares_filled: FractionalShares,
    local_shares_filled: FractionalShares,
) -> Result<bool, OffchainOrderError> {
    broker_shares_filled
        .inner()
        .gt(local_shares_filled.inner())
        .map_err(|error| {
            tracing::error!(
                ?error,
                %broker_shares_filled,
                %local_shares_filled,
                "Float comparison of cumulative fills failed"
            );
            OffchainOrderError::FillComparisonFailed {
                broker_shares_filled,
                local_shares_filled,
            }
        })
}

/// Queries the broker for the current state of an order before cancellation
/// and emits the appropriate partial-fill / fill events so the local
/// aggregate is reconciled with the broker before the terminal Cancelled
/// event. `local_filled` is the cumulative quantity already recorded in
/// the local PartiallyFilled state (None if the local state is Submitted).
///
/// Returns the events that should be emitted *before* the cancel attempt.
/// If the returned vec contains `Filled`, the caller MUST short-circuit
/// and not attempt the DELETE (the broker already filled).
async fn reconcile_pre_cancel(
    services: &dyn OrderPlacer,
    executor_order_id: &ExecutorOrderId,
    local_filled: Option<FractionalShares>,
    cancellation_reason: CancellationReason,
) -> Result<Vec<OffchainOrderEvent>, OffchainOrderError> {
    // Propagate read failures so the aggregate stays in its prior state
    // and the caller can retry. Silently bypassing reconciliation would
    // re-introduce the partial-fill loss bug the function exists to fix
    // -- a transient status-API failure right before cancel must not
    // become irreversible data loss.
    let state = services
        .get_order_status(executor_order_id)
        .await
        .map_err(|error| {
            tracing::warn!(
                %executor_order_id,
                %error,
                "Failed to read broker state for pre-cancel reconciliation; \
                 will retry without cancelling"
            );
            OffchainOrderError::PreCancelStatusFetchFailed {
                executor_order_id: executor_order_id.clone(),
            }
        })?;

    match state {
        OrderState::PartiallyFilled {
            shares_filled: broker_filled,
            avg_price,
            partially_filled_at,
            ..
        } => {
            if Positive::new(broker_filled).is_err() {
                if broker_filled == FractionalShares::ZERO {
                    return Ok(Vec::new());
                }

                return Err(OffchainOrderError::InvalidTerminalFillQuantity {
                    executor_order_id: executor_order_id.clone(),
                    shares_filled: broker_filled,
                });
            }

            // Only emit when the broker reports STRICTLY MORE fills than
            // the local aggregate already records. Equal => no-op (the
            // local state is already up to date). LESS => stale broker
            // read (the poll loop recorded fresher data); never regress
            // the recorded cumulative quantity. A comparison failure
            // propagates (fail closed, like the status-fetch failure above)
            // so the cancel retries instead of proceeding blind and
            // dropping the broker-reported fill.
            if let Some(local) = local_filled
                && !broker_fill_exceeds_local(broker_filled, local)?
            {
                tracing::debug!(
                    %executor_order_id,
                    "Broker partial-fill <= local; skipping pre-cancel reconcile"
                );
                return Ok(Vec::new());
            }

            // We need an avg_price for the event. If the broker did not
            // return one, drop the reconciliation -- without a price we
            // can't record the fill correctly. The position would be left
            // unhedged for the partial quantity, but that's safer than
            // recording a zero-price fill.
            let Some(price) = avg_price else {
                tracing::warn!(
                    %executor_order_id,
                    "Broker reports PartiallyFilled but no avg_price; will retry without cancelling"
                );
                return Err(OffchainOrderError::PreCancelPartialFillMissingAvgPrice {
                    executor_order_id: executor_order_id.clone(),
                    shares_filled: broker_filled,
                });
            };

            Ok(vec![OffchainOrderEvent::PartiallyFilled {
                shares_filled: broker_filled,
                avg_price: price,
                partially_filled_at,
            }])
        }

        OrderState::Filled {
            price, executed_at, ..
        } => {
            // The order filled completely between our last poll and the
            // cancel attempt. Record the fill so the position aggregate
            // gets the full hedge, and skip the DELETE (it would fail or
            // be no-op).
            tracing::info!(
                %executor_order_id,
                "Broker reports order fully Filled at cancel time; reconciling without DELETE"
            );
            Ok(vec![OffchainOrderEvent::Filled {
                price,
                filled_at: executed_at,
            }])
        }

        OrderState::Failed {
            error_reason,
            failed_at,
            shares_filled,
            avg_price,
        } => {
            // Order terminally failed at the broker between our last poll
            // and the cancel attempt. Emit Failed (which short-circuits
            // the DELETE -- attempting it would return 422 "not
            // cancellable" and trap the aggregate in CancelFailed retry).
            tracing::info!(
                %executor_order_id,
                ?error_reason,
                "Broker reports order Failed at cancel time; emitting Failed without DELETE"
            );
            let error =
                error_reason.unwrap_or_else(|| "Broker reported Failed at cancel time".to_string());

            reconcile_terminal_fill(
                executor_order_id,
                local_filled,
                shares_filled,
                avg_price,
                failed_at,
                OffchainOrderEvent::Failed {
                    error,
                    filled_shares: shares_filled,
                    failed_at,
                },
            )
        }

        OrderState::Cancelled {
            cancelled_at,
            shares_filled,
            avg_price,
            ..
        } => {
            tracing::info!(
                %executor_order_id,
                "Broker reports order already Cancelled at cancel time; reconciling without DELETE"
            );

            reconcile_terminal_fill(
                executor_order_id,
                local_filled,
                Some(shares_filled),
                avg_price,
                cancelled_at,
                OffchainOrderEvent::Cancelled {
                    reason: cancellation_reason,
                    filled_shares: Some(shares_filled),
                    cancelled_at,
                },
            )
        }

        OrderState::Pending | OrderState::Submitted { .. } => Ok(Vec::new()),
    }
}

/// Shared tail of the `Failed`/`Cancelled` arms of [`reconcile_pre_cancel`]:
/// applies the broker's terminal fill data ahead of `terminal_event`.
///
/// Encodes two invariants that must not drift between the arms:
/// - cumulative fills never regress -- a broker fill no newer than the local
///   record emits only the terminal event;
/// - a positive fill without an average price must NOT be dropped -- it
///   blocks with [`OffchainOrderError::PreCancelPartialFillMissingAvgPrice`]
///   so the cancel retries once the broker returns a priced fill, instead of
///   clearing the position and double-hedging the filled shares.
///
/// `broker_timestamp` is the broker event time of the terminal state
/// (`failed_at`/`cancelled_at`), used as the fill's `partially_filled_at`.
fn reconcile_terminal_fill(
    executor_order_id: &ExecutorOrderId,
    local_filled: Option<FractionalShares>,
    shares_filled: Option<FractionalShares>,
    avg_price: Option<Usd>,
    broker_timestamp: DateTime<Utc>,
    terminal_event: OffchainOrderEvent,
) -> Result<Vec<OffchainOrderEvent>, OffchainOrderError> {
    if let (Some(broker_filled), Some(avg_price)) = (shares_filled, avg_price) {
        // A zero priced fill carries nothing to record; anything else
        // non-positive is a corrupt broker value and must not be persisted
        // (classify_terminal_fill downstream would silently mask it as
        // NoFill, under-accounting the position).
        if Positive::new(broker_filled).is_err() {
            if broker_filled == FractionalShares::ZERO {
                return Ok(vec![terminal_event]);
            }
            return Err(OffchainOrderError::InvalidTerminalFillQuantity {
                executor_order_id: executor_order_id.clone(),
                shares_filled: broker_filled,
            });
        }

        if let Some(local) = local_filled
            && !broker_fill_exceeds_local(broker_filled, local)?
        {
            return Ok(vec![terminal_event]);
        }

        return Ok(vec![
            OffchainOrderEvent::PartiallyFilled {
                shares_filled: broker_filled,
                avg_price,
                partially_filled_at: broker_timestamp,
            },
            terminal_event,
        ]);
    }

    if let (Some(shares_filled), None) = (shares_filled, avg_price) {
        if Positive::new(shares_filled).is_err() {
            if shares_filled == FractionalShares::ZERO {
                return Ok(vec![terminal_event]);
            }

            return Err(OffchainOrderError::InvalidTerminalFillQuantity {
                executor_order_id: executor_order_id.clone(),
                shares_filled,
            });
        }

        // An unpriced broker fill only blocks when it reports MORE than the
        // local aggregate has already recorded (priced): an equal-or-smaller
        // unpriced fill carries no new information -- the local priced fill
        // already covers it, so the terminal event can proceed.
        if let Some(local) = local_filled
            && !broker_fill_exceeds_local(shares_filled, local)?
        {
            return Ok(vec![terminal_event]);
        }

        return Err(OffchainOrderError::PreCancelPartialFillMissingAvgPrice {
            executor_order_id: executor_order_id.clone(),
            shares_filled,
        });
    }

    Ok(vec![terminal_event])
}

/// Result of a successful order placement, with the executor-assigned ID
/// and the actual quantity placed (which may differ from the requested
/// quantity due to broker precision limits).
pub struct OrderPlacementResult {
    pub executor_order_id: ExecutorOrderId,
    pub placed_shares: Positive<FractionalShares>,
    /// Whether the broker holds the order as extended-hours. Usually echoes
    /// the requested kind, but a duplicate-`client_order_id` placement adopts
    /// the order a prior attempt already created -- possibly with different
    /// session terms (e.g. a regular-hours market retry adopting a still-live
    /// extended-hours limit order after a lost placement response). The
    /// aggregate must record THIS value so the regular-open cancel-and-replace
    /// sweep (which keys off `is_extended_hours`) converges the adopted order.
    pub is_extended_hours: bool,
    /// The broker-held limit price, if any. Same adoption semantics as
    /// `is_extended_hours`.
    pub limit_price: Option<Positive<Usd>>,
}

/// Type-erased order placement capability used by the durable placement path
/// ([`place_offchain_order_at_broker`]) -- the trade-processing context, the
/// hedge job, and the CLI each hold one. The `OffchainOrder` aggregate keeps
/// this as its service type for cancel pre-reconciliation and placement-adjacent
/// recovery paths, while `Place` itself remains a pure intent-recording command.
///
/// This trait exists because the `Executor` trait has associated types
/// (`Error`, `OrderId`, `Ctx`) which make it non-object-safe - you cannot
/// write `Arc<dyn Executor>`. This trait provides the minimal surface needed
/// by the aggregate with erased error/ID types, allowing different executor
/// implementations to be used via `Arc<dyn OrderPlacer>`.
#[async_trait]
pub trait OrderPlacer: Send + Sync {
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>;

    async fn place_limit_order(
        &self,
        order: LimitOrder,
    ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>;

    async fn cancel_order(
        &self,
        executor_order_id: &ExecutorOrderId,
    ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>;

    /// Fetches the latest trade price for a symbol, used to compute limit
    /// prices for extended-hours counter-trades. Returns `None` when the
    /// executor does not support market data lookups.
    async fn fetch_latest_trade_price(
        &self,
        _symbol: &Symbol,
    ) -> Result<Option<st0x_execution::Positive<Usd>>, Box<dyn std::error::Error + Send + Sync>>
    {
        Ok(None)
    }

    /// Returns the current market session. Used by hedge jobs to re-check
    /// the session at execution time so a queued job does not submit the
    /// wrong order type across the 9:30/16:00 ET boundary. The default
    /// returns `Regular`, which is the safe assumption for executors
    /// without session awareness (e.g. dry-run).
    async fn market_session(
        &self,
    ) -> Result<st0x_execution::MarketSession, Box<dyn std::error::Error + Send + Sync>> {
        Ok(st0x_execution::MarketSession::Regular)
    }

    /// Queries the broker for the current state of an order. Used by the
    /// `CancelOrder` handler's `reconcile_pre_cancel` to apply any fill that
    /// landed between the last poll and the cancel.
    ///
    /// The default deliberately FAILS rather than fabricating a `Submitted`
    /// state: a fabricated-`Submitted` default would make an implementer that
    /// forgot to query the broker silently skip reconciliation and drop a fill
    /// -- the exact partial-fill loss `reconcile_pre_cancel` exists to prevent.
    /// `reconcile_pre_cancel` maps this error to `PreCancelStatusFetchFailed`,
    /// which keeps the order in its prior state and retries instead of
    /// cancelling blind. Real implementations (`ExecutorOrderPlacer`) override
    /// this to delegate to the executor.
    async fn get_order_status(
        &self,
        _executor_order_id: &ExecutorOrderId,
    ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>> {
        Err("get_order_status not implemented for this OrderPlacer".into())
    }
}

/// Bridges `Executor` (which has associated types and is not object-safe)
/// to `OrderPlacer` (object-safe).
pub(crate) struct ExecutorOrderPlacer<E>(pub E);

#[async_trait]
impl<E: Executor> OrderPlacer for ExecutorOrderPlacer<E> {
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
        let placement = self.0.place_market_order(order).await?;
        Ok(OrderPlacementResult {
            executor_order_id: ExecutorOrderId::new(&placement.order_id),
            placed_shares: placement.shares,
            is_extended_hours: placement.extended_hours,
            limit_price: placement.limit_price,
        })
    }

    async fn place_limit_order(
        &self,
        order: LimitOrder,
    ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
        let placement = self.0.place_limit_order(order).await?;
        Ok(OrderPlacementResult {
            executor_order_id: ExecutorOrderId::new(&placement.order_id),
            placed_shares: placement.shares,
            is_extended_hours: placement.extended_hours,
            limit_price: placement.limit_price,
        })
    }

    async fn cancel_order(
        &self,
        executor_order_id: &ExecutorOrderId,
    ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
        let order_id = self.0.parse_order_id(executor_order_id.as_ref())?;
        Ok(self.0.cancel_order(&order_id).await?)
    }

    async fn fetch_latest_trade_price(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<st0x_execution::Positive<Usd>>, Box<dyn std::error::Error + Send + Sync>>
    {
        Ok(self.0.fetch_latest_trade_price(symbol).await?)
    }

    async fn market_session(
        &self,
    ) -> Result<st0x_execution::MarketSession, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.0.market_session().await?)
    }

    async fn get_order_status(
        &self,
        executor_order_id: &ExecutorOrderId,
    ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>> {
        let order_id = self.0.parse_order_id(executor_order_id.as_ref())?;
        Ok(self.0.get_order_status(&order_id).await?)
    }
}

#[cfg(test)]
pub(crate) fn noop_order_placer() -> Arc<dyn OrderPlacer> {
    struct Noop;

    #[async_trait]
    impl OrderPlacer for Noop {
        async fn place_market_order(
            &self,
            order: MarketOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            Ok(OrderPlacementResult {
                executor_order_id: ExecutorOrderId::new("noop"),
                placed_shares: noop_placed_shares(order.shares),
                is_extended_hours: false,
                limit_price: None,
            })
        }

        async fn place_limit_order(
            &self,
            order: LimitOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            Ok(OrderPlacementResult {
                executor_order_id: ExecutorOrderId::new("noop-limit"),
                placed_shares: noop_placed_shares(order.shares),
                is_extended_hours: order.extended_hours,
                limit_price: Some(order.limit_price),
            })
        }

        async fn cancel_order(
            &self,
            _executor_order_id: &ExecutorOrderId,
        ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
            Ok(CancellationOutcome::Requested)
        }

        async fn get_order_status(
            &self,
            executor_order_id: &ExecutorOrderId,
        ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>> {
            // No-op placer reports the order still live, so pre-cancel
            // reconciliation finds nothing to apply (matches the prior default).
            Ok(st0x_execution::OrderState::Submitted {
                order_id: executor_order_id.clone(),
            })
        }
    }

    Arc::new(Noop)
}

/// Returns a placed_shares value distinct from the requested shares,
/// simulating broker truncation. Used so tests can verify the system
/// persists the broker-accepted quantity, not the original request.
#[cfg(test)]
pub(crate) fn noop_placed_shares(
    requested: Positive<FractionalShares>,
) -> Positive<FractionalShares> {
    let original = requested.inner().inner();
    let offset = st0x_float_macro::float!(0.001);
    let truncated = (original - offset).expect("subtraction should not fail");

    Positive::new(FractionalShares::new(truncated)).expect("truncated shares should be positive")
}

/// Determines whether a counter-trade is placed as a market order (regular
/// hours) or a limit order with `extended_hours: true` (pre-market /
/// after-hours).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CounterTradeOrderKind {
    Market,
    ExtendedHoursLimit { limit_price: Positive<Usd> },
}

impl CounterTradeOrderKind {
    fn market_session(&self) -> MarketSession {
        match self {
            Self::Market => MarketSession::Regular,
            Self::ExtendedHoursLimit { .. } => MarketSession::Extended,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OffchainOrderCommand {
    Place {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        /// Idempotency key forwarded to the broker so apalis retries of
        /// the same `PlaceHedge` job do not produce a second order if the
        /// first placement's response is lost in flight.
        client_order_id: ClientOrderId,
        kind: CounterTradeOrderKind,
    },
    /// Test/fixture-only: identical to `Place` but takes `placed_at`
    /// explicitly instead of stamping `Utc::now()`, so fixture seeding can
    /// backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    PlaceAt {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        client_order_id: ClientOrderId,
        kind: CounterTradeOrderKind,
        placed_at: DateTime<Utc>,
    },
    /// Request broker cancellation for a submitted order and persist the
    /// in-flight cancellation state. The order becomes terminal only after the
    /// broker later reports `Cancelled`.
    CancelOrder { reason: CancellationReason },
    ConfirmCancellation {
        filled_shares: FractionalShares,
        cancelled_at: DateTime<Utc>,
    },
    /// Records a broker-side cancellation observed without a durable local
    /// cancel request (for example, an operator cancellation).
    MarkUnrequestedCancellation {
        filled_shares: FractionalShares,
        cancelled_at: DateTime<Utc>,
    },
    UpdatePartialFill {
        shares_filled: FractionalShares,
        avg_price: Usd,
        /// Broker-reported time of the fill (e.g. the order's `updated_at` /
        /// terminal-state timestamp), persisted on the `PartiallyFilled`
        /// event. Must NOT be the wall-clock time the caller happens to run:
        /// it flows into `Position.last_updated` and recency/ordering logic.
        partially_filled_at: DateTime<Utc>,
    },
    CompleteFill {
        price: Usd,
        /// Broker-reported execution time, persisted as the `Filled` event's
        /// `filled_at`. Must NOT be the wall-clock time the caller happens to
        /// run: it feeds `terminal_position_finalization`'s
        /// `broker_timestamp` and `Position.last_updated`.
        filled_at: DateTime<Utc>,
    },
    /// Outcome command: the broker accepted the placement. Fed back by the
    /// durable placement path after `place_market_order`, carrying the
    /// executor-assigned id and the broker-accepted `placed_shares`. Idempotent
    /// against a job retry: a no-op once the order has left `Pending`.
    MarkAccepted {
        executor_order_id: ExecutorOrderId,
        placed_shares: Positive<FractionalShares>,
        submitted_at: DateTime<Utc>,
        /// Broker-adopted session terms. Usually match the requested
        /// [`CounterTradeOrderKind`], but may differ when a duplicate
        /// client_order_id adopts an existing broker order.
        market_session: MarketSession,
        limit_price: Option<Positive<Usd>>,
    },
    /// Outcome command for a placement-initiated failure: the broker call errored
    /// while the placement path still held a `Pending` order. Honoured only from
    /// `Pending`, so a stale attempt cannot fail a live order a concurrent attempt
    /// already accepted. The poll-rejection path instead uses `MarkFailed`, which
    /// may fail a live `Submitted`/`PartiallyFilled` order.
    MarkPlacementFailed { error: String },
    MarkFailed {
        error: String,
        /// Broker-reported cumulative fill quantity. `None` means the caller
        /// has no persisted evidence for the actual fill.
        filled_shares: Option<FractionalShares>,
        /// Broker-reported failure time when available; callers without a
        /// broker timestamp pass their observation time, which is still
        /// closer to the truth than stamping inside the handler after
        /// queueing delays.
        failed_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OffchainOrderEvent {
    Placed {
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        placed_at: DateTime<Utc>,
        /// Whether this order was placed during extended hours as a limit
        /// order. Used by the cancel-and-replace logic to avoid cancelling
        /// regular-hours market orders. Defaults to `false` for events
        /// persisted before this field existed.
        #[serde(default)]
        is_extended_hours: bool,
        /// The limit price submitted to the broker for an extended-hours order
        /// (`None` for market orders). Audit-only: not applied to entity state,
        /// recorded so the actual submitted price is reconstructable from the
        /// event stream. `#[serde(default)]` for events predating this field.
        #[serde(default)]
        limit_price: Option<Positive<Usd>>,
        /// The broker idempotency key submitted with this placement. Audit-only.
        /// `#[serde(default)]` (None) for events predating this field.
        #[serde(default)]
        client_order_id: Option<ClientOrderId>,
    },
    /// Legacy broker-acceptance event. Predates the durable-job extraction,
    /// where `Place` did the broker call inline and emitted this alongside
    /// `Placed`. Retained only so orders placed before the extraction still
    /// replay; new placements emit [`Accepted`](Self::Accepted) instead.
    Submitted {
        executor_order_id: ExecutorOrderId,
        submitted_at: DateTime<Utc>,
    },
    /// The broker accepted the placement, assigning `executor_order_id` and the
    /// `placed_shares` it actually accepted. This is usually <= requested (the
    /// broker may truncate to its precision) but can exceed the request on a
    /// broker over-fill, which ADR 0009 records as an acceptance rather than a
    /// failure. Emitted by the durable placement path after the external
    /// `place_market_order` call, now that `Place` only records intent. Carries
    /// `placed_shares` because the now-pure `Placed` event can only record the
    /// requested quantity.
    Accepted {
        executor_order_id: ExecutorOrderId,
        placed_shares: Positive<FractionalShares>,
        submitted_at: DateTime<Utc>,
        #[serde(default = "regular_market_session")]
        market_session: MarketSession,
        #[serde(default)]
        limit_price: Option<Positive<Usd>>,
    },
    PartiallyFilled {
        shares_filled: FractionalShares,
        avg_price: Usd,
        partially_filled_at: DateTime<Utc>,
    },
    CancelRequested {
        reason: CancellationReason,
        cancel_requested_at: DateTime<Utc>,
    },
    Filled {
        price: Usd,
        filled_at: DateTime<Utc>,
    },
    Failed {
        error: String,
        /// Broker-reported cumulative fill quantity. Absent on legacy events,
        /// where the actual fill remains explicitly unknown unless an earlier
        /// partial-fill event proves it.
        #[serde(default)]
        filled_shares: Option<FractionalShares>,
        failed_at: DateTime<Utc>,
    },
    Cancelled {
        reason: CancellationReason,
        #[serde(default)]
        filled_shares: Option<FractionalShares>,
        cancelled_at: DateTime<Utc>,
    },
}

impl DomainEvent for OffchainOrderEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Placed { .. } => "OffchainOrderEvent::Placed".to_string(),
            Self::Submitted { .. } => "OffchainOrderEvent::Submitted".to_string(),
            Self::Accepted { .. } => "OffchainOrderEvent::Accepted".to_string(),
            Self::PartiallyFilled { .. } => "OffchainOrderEvent::PartiallyFilled".to_string(),
            Self::CancelRequested { .. } => "OffchainOrderEvent::CancelRequested".to_string(),
            Self::Filled { .. } => "OffchainOrderEvent::Filled".to_string(),
            Self::Failed { .. } => "OffchainOrderEvent::Failed".to_string(),
            Self::Cancelled { .. } => "OffchainOrderEvent::Cancelled".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct OffchainOrderId(Uuid);

impl std::fmt::Display for OffchainOrderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for OffchainOrderId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

impl OffchainOrderId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wraps a caller-supplied UUID, for deterministic identifiers (e.g.
    /// fixture seeding) where [`Self::new`]'s randomness isn't wanted.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// Exposes the wrapped UUID so callers can derive other identifiers
    /// (e.g. a broker-side `client_order_id`) without going through a
    /// fallible string roundtrip.
    pub(crate) fn as_uuid(&self) -> Uuid {
        self.0
    }
}

/// Returned by [`OffchainOrder::try_into_trade`] when the order cannot be
/// rendered as a dashboard trade.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TradeConversionError {
    #[error("pending OffchainOrder cannot be rendered as a Trade")]
    Pending,
    #[error("submitted OffchainOrder cannot be rendered as a Trade")]
    Submitted,
    #[error("partially filled OffchainOrder cannot be rendered as a Trade")]
    PartiallyFilled,
    #[error("cancelling OffchainOrder cannot be rendered as a Trade")]
    Cancelling,
    #[error("failed to calculate the remaining counter-trade shares: {0}")]
    Arithmetic(#[from] FloatError),
    #[error("failed counter-trade shares cannot be negative: {0}")]
    NegativeShares(#[from] NotNonNegative<FractionalShares>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub enum OffchainOrderError {
    #[error(
        "Cannot update or complete fill: order has not been \
         submitted to broker yet"
    )]
    NotSubmitted,
    #[error("Cannot update order: order has already been completed (filled, failed, or cancelled)")]
    AlreadyCompleted,
    #[error("Cannot confirm cancellation: broker cancellation has not been requested")]
    CancellationNotRequested,
    #[error(
        "Broker reported an invalid (non-positive, non-zero) terminal fill quantity \
         {shares_filled} for order {executor_order_id}; refusing to record it"
    )]
    InvalidTerminalFillQuantity {
        executor_order_id: ExecutorOrderId,
        shares_filled: FractionalShares,
    },
    #[error("Order has not been placed yet")]
    NotPlaced,
    #[error(
        "Place retry payload diverges from the existing order: a re-`Place` must \
         replay the original symbol, direction, and executor"
    )]
    PlacePayloadMismatch,
    /// Pre-cancel broker status query failed. Surfaced as an error so the
    /// aggregate stays in its prior state and the caller retries -- silent
    /// bypass would lose any partial fills that occurred between the last
    /// poll and the cancel attempt. The underlying broker error is a
    /// non-serializable `Box<dyn Error>` (this enum must satisfy the CQRS
    /// `Clone + Serialize + Deserialize + PartialEq + Eq` derives), so it is
    /// logged at the call site rather than carried here.
    #[error("Failed to read pre-cancel broker state for order {executor_order_id}")]
    PreCancelStatusFetchFailed { executor_order_id: ExecutorOrderId },
    #[error(
        "Broker reported partial fill of {shares_filled} shares for order \
         {executor_order_id} without an average price"
    )]
    PreCancelPartialFillMissingAvgPrice {
        executor_order_id: ExecutorOrderId,
        shares_filled: FractionalShares,
    },
    /// Comparing the broker-reported cumulative fill against the locally
    /// recorded quantity failed. Carries both operands so the failing
    /// comparison is reproducible from the error alone; the underlying
    /// `rain_math_float::FloatError` is not serializable, so it is logged
    /// at the comparison site ([`broker_fill_exceeds_local`]) instead.
    #[error(
        "Failed to compare broker-reported fill {broker_shares_filled} \
         against locally recorded fill {local_shares_filled}"
    )]
    FillComparisonFailed {
        broker_shares_filled: FractionalShares,
        local_shares_filled: FractionalShares,
    },
    /// The broker rejected or failed the cancellation request. The underlying
    /// broker error is a non-serializable `Box<dyn Error>`, so it is logged
    /// at the call site rather than carried here.
    #[error("Broker rejected the cancellation request for order {executor_order_id}")]
    CancelFailed { executor_order_id: ExecutorOrderId },
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use st0x_event_sorcery::{AggregateError, LifecycleError, StoreBuilder, TestStore, replay};

    use super::*;
    use st0x_float_macro::float;

    fn failing_order_placer() -> Arc<dyn OrderPlacer> {
        struct Failing;

        #[async_trait]
        impl OrderPlacer for Failing {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("Broker rejected order".into())
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("Broker rejected order".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                Ok(CancellationOutcome::Requested)
            }
        }

        Arc::new(Failing)
    }

    fn limit_failing_order_placer() -> Arc<dyn OrderPlacer> {
        struct LimitFailing;

        #[async_trait]
        impl OrderPlacer for LimitFailing {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("extended-hours command must use place_limit_order");
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("Limit order rejected".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                Ok(CancellationOutcome::Requested)
            }
        }

        Arc::new(LimitFailing)
    }

    fn place_command() -> OffchainOrderCommand {
        OffchainOrderCommand::Place {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::DryRun,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            kind: CounterTradeOrderKind::Market,
        }
    }

    /// Drives an order to `Submitted` the way the durable placement path does:
    /// a pure `Place` (-> Pending) followed by the broker-acceptance feedback
    /// `MarkAccepted`. The accepted quantity is `noop_placed_shares(100)`, so
    /// the resulting `Submitted` carries the broker-working amount, not 100.
    async fn place_and_submit(store: &TestStore<OffchainOrder>, id: &OffchainOrderId) {
        let requested = Positive::new(FractionalShares::new(float!(100))).unwrap();
        store.send(id, place_command()).await.unwrap();
        store
            .send(
                id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("TEST-ACCEPT"),
                    placed_shares: noop_placed_shares(requested),
                    submitted_at: Utc::now(),
                    market_session: MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();
    }

    #[test]
    fn placed_event_records_order_terms_and_defaults_for_legacy_events() {
        let event = OffchainOrderEvent::Placed {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::DryRun,
            placed_at: Utc::now(),
            is_extended_hours: true,
            limit_price: Some(Positive::new(Usd::new(float!(195.25))).unwrap()),
            client_order_id: Some(ClientOrderId::from_uuid(uuid::Uuid::new_v4())),
        };

        // The submitted terms are recorded on the event for audit.
        let mut value = serde_json::to_value(&event).unwrap();
        assert!(
            !value["Placed"]["limit_price"].is_null(),
            "limit_price must be recorded on the Placed event"
        );
        assert!(
            !value["Placed"]["client_order_id"].is_null(),
            "client_order_id must be recorded on the Placed event"
        );

        // Events persisted before these fields existed (no keys) deserialize
        // with the fields defaulted to None rather than failing.
        let placed = value["Placed"].as_object_mut().unwrap();
        placed.remove("limit_price");
        placed.remove("client_order_id");
        let legacy: OffchainOrderEvent = serde_json::from_value(value).unwrap();
        assert!(
            matches!(
                legacy,
                OffchainOrderEvent::Placed {
                    limit_price: None,
                    client_order_id: None,
                    ..
                }
            ),
            "legacy Placed event must default the audit terms to None, got: {legacy:?}"
        );
    }

    #[test]
    fn cancelled_with_retained_fill_finalizes_with_fill_time_not_cancel_time() {
        // A partial fill at T1 followed by cancellation at T2 must finalize
        // the position with the broker's fill time T1 -- the cancellation
        // time is when the order died, not when the shares were executed.
        let fill_time = "2026-01-05T14:30:00Z".parse::<DateTime<Utc>>().unwrap();
        let cancel_time = "2026-01-06T14:32:01Z".parse::<DateTime<Utc>>().unwrap();
        let order = OffchainOrder::Cancelled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(2))).unwrap(),
            requested_shares: Some(Positive::new(FractionalShares::new(float!(2))).unwrap()),
            retained_fill: Some(RetainedFill::priced(
                FractionalShares::new(float!(0.5)),
                Usd::new(float!(195.25)),
                fill_time,
            )),
            filled_shares: Some(FractionalShares::new(float!(0.5))),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            executor_order_id: ExecutorOrderId::new("cancelled-with-fill"),
            reason: CancellationReason::MarketOpenReplacement,
            placed_at: fill_time,
            cancelled_at: cancel_time,
        };

        let finalization =
            terminal_position_finalization(&order).expect("terminal Cancelled order must classify");
        let TerminalPositionFinalization::Complete {
            broker_timestamp, ..
        } = finalization
        else {
            panic!("priced retained fill must classify as Complete, got: {finalization:?}");
        };
        assert_eq!(
            broker_timestamp, fill_time,
            "finalization must stamp the broker fill time, not the cancellation time"
        );
    }

    #[test]
    fn failed_with_retained_fill_finalizes_with_fill_time_not_failure_time() {
        // Mirror of the Cancelled case: a partial fill at T1 followed by a
        // rejection at T2 must finalize the position with the broker's fill
        // time T1, including on a retry that loads the already-Failed order
        // after MarkFailed persisted.
        let fill_time = "2026-01-05T14:30:00Z".parse::<DateTime<Utc>>().unwrap();
        let failure_time = "2026-01-06T14:32:01Z".parse::<DateTime<Utc>>().unwrap();
        let order = OffchainOrder::Failed {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(2))).unwrap(),
            requested_shares: Some(Positive::new(FractionalShares::new(float!(2))).unwrap()),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            retained_fill: Some(RetainedFill::priced(
                FractionalShares::new(float!(0.5)),
                Usd::new(float!(195.25)),
                fill_time,
            )),
            filled_shares: Some(FractionalShares::new(float!(0.5))),
            executor_order_id: Some(ExecutorOrderId::new("failed-with-fill")),
            error: "broker rejected remainder".to_string(),
            placed_at: fill_time,
            failed_at: failure_time,
        };

        let finalization =
            terminal_position_finalization(&order).expect("terminal Failed order must classify");
        let TerminalPositionFinalization::Complete {
            broker_timestamp, ..
        } = finalization
        else {
            panic!("priced retained fill must classify as Complete, got: {finalization:?}");
        };
        assert_eq!(
            broker_timestamp, fill_time,
            "finalization must stamp the broker fill time, not the failure time"
        );
    }

    #[test]
    fn failed_trade_preserves_requested_accepted_and_filled_shares() {
        let fill_time = "2026-01-05T14:30:00Z".parse::<DateTime<Utc>>().unwrap();
        let failure_time = "2026-01-06T14:32:01Z".parse::<DateTime<Utc>>().unwrap();
        let order = OffchainOrder::Failed {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(1.5))).unwrap(),
            requested_shares: Some(Positive::new(FractionalShares::new(float!(2))).unwrap()),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            retained_fill: Some(RetainedFill::priced(
                FractionalShares::new(float!(0.5)),
                Usd::new(float!(195.25)),
                fill_time,
            )),
            filled_shares: Some(FractionalShares::new(float!(0.5))),
            executor_order_id: Some(ExecutorOrderId::new("failed-with-fill")),
            error: "broker rejected remainder".to_string(),
            placed_at: fill_time,
            failed_at: failure_time,
        };

        let trade = order
            .try_into_trade(&OffchainOrderId::new())
            .expect("valid terminal failure should convert");
        assert_eq!(trade.occurred_at, failure_time);
        assert!(trade.shares.inner().inner().eq(float!(2)).unwrap());
        match trade.outcome {
            TradeOutcome::Failed {
                error,
                accepted_shares,
                filled_shares,
                remaining_shares,
                excess_shares,
            } => {
                assert_eq!(error, "broker rejected remainder");
                assert!(
                    accepted_shares
                        .unwrap()
                        .inner()
                        .inner()
                        .eq(float!(1.5))
                        .unwrap()
                );
                assert!(
                    filled_shares
                        .unwrap()
                        .inner()
                        .inner()
                        .eq(float!(0.5))
                        .unwrap()
                );
                assert!(
                    remaining_shares
                        .unwrap()
                        .inner()
                        .inner()
                        .eq(float!(1))
                        .unwrap()
                );
                assert!(excess_shares.unwrap().inner().inner().is_zero().unwrap());
            }
            TradeOutcome::Filled | TradeOutcome::Cancelled { .. } => {
                panic!("failed order must retain its failure outcome")
            }
        }
    }

    #[test]
    fn failed_trade_reports_retained_fill_larger_than_order_as_excess() {
        let failed_at = Utc::now();
        let order = OffchainOrder::Failed {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            requested_shares: Some(Positive::new(FractionalShares::new(float!(1))).unwrap()),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            retained_fill: Some(RetainedFill::priced(
                FractionalShares::new(float!(1.5)),
                Usd::new(float!(195.25)),
                failed_at,
            )),
            filled_shares: Some(FractionalShares::new(float!(1.5))),
            executor_order_id: Some(ExecutorOrderId::new("invalid-retained-fill")),
            error: "broker reported an impossible fill".to_string(),
            placed_at: failed_at,
            failed_at,
        };

        let trade = order
            .try_into_trade(&OffchainOrderId::new())
            .expect("overfilled terminal failures must remain visible");
        let TradeOutcome::Failed {
            filled_shares,
            remaining_shares,
            excess_shares,
            ..
        } = trade.outcome
        else {
            panic!("failed order must retain its failure outcome");
        };
        assert!(
            filled_shares
                .unwrap()
                .inner()
                .inner()
                .eq(float!(1.5))
                .unwrap()
        );
        assert!(remaining_shares.unwrap().inner().inner().is_zero().unwrap());
        assert!(
            excess_shares
                .unwrap()
                .inner()
                .inner()
                .eq(float!(0.5))
                .unwrap()
        );
    }

    #[test]
    fn cancelled_trade_distinguishes_explicit_zero_fill() {
        let cancelled_at = Utc::now();
        let accepted = Positive::new(FractionalShares::new(float!(1.5))).unwrap();
        let requested = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order = OffchainOrder::Cancelled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: accepted,
            requested_shares: Some(requested),
            retained_fill: None,
            filled_shares: Some(FractionalShares::ZERO),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            executor_order_id: ExecutorOrderId::new("cancelled-without-fill"),
            reason: CancellationReason::MarketOpenReplacement,
            placed_at: cancelled_at,
            cancelled_at,
        };

        let trade = order.try_into_trade(&OffchainOrderId::new()).unwrap();
        assert_eq!(trade.shares, requested);
        let TradeOutcome::Cancelled {
            accepted_shares,
            filled_shares,
            remaining_shares,
            excess_shares,
        } = trade.outcome
        else {
            panic!("cancelled order must retain its cancellation outcome");
        };
        assert_eq!(accepted_shares, Some(accepted));
        assert_eq!(
            filled_shares,
            Some(NonNegative::new(FractionalShares::ZERO).unwrap())
        );
        assert_eq!(
            remaining_shares,
            Some(NonNegative::new(accepted.inner()).unwrap())
        );
        assert_eq!(
            excess_shares,
            Some(NonNegative::new(FractionalShares::ZERO).unwrap())
        );
    }

    #[test]
    fn cancelled_trade_preserves_partial_fill_provenance() {
        let cancelled_at = Utc::now();
        let order = OffchainOrder::Cancelled {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            requested_shares: Some(Positive::new(FractionalShares::new(float!(1.25))).unwrap()),
            retained_fill: Some(RetainedFill::priced(
                FractionalShares::new(float!(0.25)),
                Usd::new(float!(195.25)),
                cancelled_at,
            )),
            filled_shares: Some(FractionalShares::new(float!(0.25))),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            executor_order_id: ExecutorOrderId::new("cancelled-with-partial-fill"),
            reason: CancellationReason::MarketOpenReplacement,
            placed_at: cancelled_at,
            cancelled_at,
        };

        let trade = order.try_into_trade(&OffchainOrderId::new()).unwrap();
        let TradeOutcome::Cancelled {
            filled_shares,
            remaining_shares,
            excess_shares,
            ..
        } = trade.outcome
        else {
            panic!("cancelled order must retain its cancellation outcome");
        };
        assert!(
            filled_shares
                .unwrap()
                .inner()
                .inner()
                .eq(float!(0.25))
                .unwrap()
        );
        assert!(
            remaining_shares
                .unwrap()
                .inner()
                .inner()
                .eq(float!(0.75))
                .unwrap()
        );
        assert!(excess_shares.unwrap().inner().inner().is_zero().unwrap());
    }

    #[test]
    fn legacy_cancelled_state_keeps_fill_provenance_unknown() {
        let legacy_payload = json!({
            "Cancelled": {
                "symbol": "AAPL",
                "shares": "1",
                "direction": "Sell",
                "executor": "DryRun",
                "executor_order_id": "cancelled-legacy",
                "reason": "MarketOpenReplacement",
                "placed_at": "2026-01-01T00:00:00Z",
                "cancelled_at": "2026-01-01T00:00:01Z"
            }
        });

        let state: OffchainOrder = serde_json::from_value(legacy_payload).unwrap();
        let trade = state.try_into_trade(&OffchainOrderId::new()).unwrap();
        let TradeOutcome::Cancelled {
            accepted_shares,
            filled_shares,
            remaining_shares,
            excess_shares,
        } = trade.outcome
        else {
            panic!("legacy cancelled state must retain its cancellation outcome");
        };
        assert_eq!(accepted_shares, None);
        assert_eq!(filled_shares, None);
        assert_eq!(remaining_shares, None);
        assert_eq!(excess_shares, None);
    }

    #[test]
    fn failed_trade_does_not_regress_earlier_cumulative_fill_evidence() {
        let failed_at = Utc::now();
        let order = OffchainOrder::Failed {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(2))).unwrap(),
            requested_shares: Some(Positive::new(FractionalShares::new(float!(2))).unwrap()),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            retained_fill: Some(RetainedFill::priced(
                FractionalShares::new(float!(0.5)),
                Usd::new(float!(195.25)),
                failed_at,
            )),
            filled_shares: Some(FractionalShares::new(float!(0.25))),
            executor_order_id: Some(ExecutorOrderId::new("stale-terminal-fill")),
            error: "broker rejected remainder".to_string(),
            placed_at: failed_at,
            failed_at,
        };

        let trade = order.try_into_trade(&OffchainOrderId::new()).unwrap();
        let TradeOutcome::Failed { filled_shares, .. } = trade.outcome else {
            panic!("failed order must retain its failure outcome");
        };
        assert!(
            filled_shares
                .unwrap()
                .inner()
                .inner()
                .eq(float!(0.5))
                .unwrap()
        );
    }

    #[test]
    fn legacy_failed_state_without_fill_fields_deserializes() {
        // Materialized `Failed` payloads persisted before provenance tracking
        // lack the requested, retained-fill, and explicit-fill keys. They must
        // deserialize as unknown rather than inventing zero quantities.
        let legacy_payload = json!({
            "Failed": {
                "symbol": "AAPL",
                "shares": "100",
                "direction": "Buy",
                "executor": "DryRun",
                "error": "broker rejected",
                "placed_at": "2026-01-01T00:00:00Z",
                "failed_at": "2026-01-01T00:00:01Z",
            }
        });

        let state: OffchainOrder = serde_json::from_value(legacy_payload).unwrap();
        assert!(
            matches!(
                state,
                OffchainOrder::Failed {
                    retained_fill: None,
                    executor_order_id: None,
                    ..
                }
            ),
            "legacy Failed payload must default fill metadata to None, got: {state:?}"
        );
        let trade = state.try_into_trade(&OffchainOrderId::new()).unwrap();
        let TradeOutcome::Failed {
            accepted_shares,
            filled_shares,
            remaining_shares,
            excess_shares,
            ..
        } = trade.outcome
        else {
            panic!("legacy failed state must retain its failure outcome");
        };
        assert!(trade.shares.inner().inner().eq(float!(100)).unwrap());
        assert_eq!(accepted_shares, None);
        assert_eq!(filled_shares, None);
        assert_eq!(remaining_shares, None);
        assert_eq!(excess_shares, None);
    }

    #[test]
    fn legacy_extended_hours_state_deserializes_to_market_session() {
        let submitted_at = Utc::now();
        let submitted = OffchainOrder::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
            requested_shares: None,
            direction: Direction::Buy,
            executor: SupportedExecutor::DryRun,
            executor_order_id: ExecutorOrderId::new("broker-123"),
            placed_at: submitted_at,
            submitted_at,
            market_session: MarketSession::Extended,
        };

        let mut legacy_payload = serde_json::to_value(submitted).unwrap();
        let submitted = legacy_payload
            .get_mut("Submitted")
            .and_then(serde_json::Value::as_object_mut)
            .expect("Submitted variant serializes as an object");
        submitted.remove("market_session");
        submitted.insert("is_extended_hours".to_string(), json!(true));

        let state: OffchainOrder = serde_json::from_value(legacy_payload).unwrap();
        assert!(
            matches!(
                state,
                OffchainOrder::Submitted {
                    market_session: MarketSession::Extended,
                    ..
                }
            ),
            "legacy is_extended_hours=true must deserialize as Extended, got: {state:?}"
        );
    }

    #[tokio::test]
    async fn place_order_transitions_to_submitted() {
        let placer = noop_order_placer();
        let inner = place_at_broker(placer.as_ref())
            .await
            .expect("placement helper must produce an order");
        assert!(matches!(inner, OffchainOrder::Submitted { .. }));

        let expected =
            noop_placed_shares(Positive::new(FractionalShares::new(float!(100))).unwrap());
        assert_eq!(
            inner.shares(),
            expected,
            "Persisted shares should reflect the broker-accepted quantity, not the original request"
        );
        assert!(matches!(
            inner,
            OffchainOrder::Submitted {
                requested_shares: Some(requested),
                ..
            } if requested.inner().inner().eq(float!(100)).unwrap()
        ));
    }

    #[tokio::test]
    async fn failed_trade_replays_requested_accepted_and_fill_provenance() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        let accepted = Positive::new(FractionalShares::new(float!(50))).unwrap();
        let filled = FractionalShares::new(float!(25));

        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("TEST-ACCEPT"),
                    placed_shares: accepted,
                    submitted_at: Utc::now(),
                    market_session: MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "broker rejected remainder".to_string(),
                    filled_shares: Some(filled),
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let trade = store
            .load(&id)
            .await
            .unwrap()
            .unwrap()
            .try_into_trade(&id)
            .unwrap();
        assert!(trade.shares.inner().inner().eq(float!(100)).unwrap());
        let TradeOutcome::Failed {
            accepted_shares,
            filled_shares,
            remaining_shares,
            excess_shares,
            ..
        } = trade.outcome
        else {
            panic!("failed order must retain its failure outcome");
        };
        assert_eq!(accepted_shares, Some(accepted));
        assert_eq!(
            filled_shares,
            Some(NonNegative::new(FractionalShares::new(float!(25))).unwrap())
        );
        assert_eq!(
            remaining_shares,
            Some(NonNegative::new(FractionalShares::new(float!(25))).unwrap())
        );
        assert_eq!(
            excess_shares,
            Some(NonNegative::new(FractionalShares::ZERO).unwrap())
        );
    }

    #[tokio::test]
    async fn pre_acceptance_failure_keeps_broker_provenance_unknown() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkPlacementFailed {
                    error: "broker did not accept order".to_string(),
                },
            )
            .await
            .unwrap();

        let trade = store
            .load(&id)
            .await
            .unwrap()
            .unwrap()
            .try_into_trade(&id)
            .unwrap();
        let TradeOutcome::Failed {
            accepted_shares,
            filled_shares,
            remaining_shares,
            excess_shares,
            ..
        } = trade.outcome
        else {
            panic!("failed order must retain its failure outcome");
        };
        assert_eq!(accepted_shares, None);
        assert_eq!(filled_shares, None);
        assert_eq!(remaining_shares, None);
        assert_eq!(excess_shares, None);
    }

    /// Builds a real store and runs the durable placement path against
    /// `placer`, returning the resulting order state. This is the single broker
    /// call site, so the placement outcomes are exercised here rather than
    /// through the (now pure) `Place` handler.
    async fn place_at_broker(placer: &dyn OrderPlacer) -> Option<OffchainOrder> {
        place_at_broker_with_kind(placer, CounterTradeOrderKind::Market).await
    }

    async fn place_at_broker_with_kind(
        placer: &dyn OrderPlacer,
        kind: CounterTradeOrderKind,
    ) -> Option<OffchainOrder> {
        let pool = crate::test_utils::setup_test_db().await;
        let (store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(noop_order_placer())
            .await
            .unwrap();

        place_offchain_order_at_broker(
            &store,
            placer,
            &OffchainOrderId::new(),
            OffchainOrderPlacement::with_kind(
                Symbol::new("AAPL").unwrap(),
                Positive::new(FractionalShares::new(float!(100))).unwrap(),
                Direction::Buy,
                SupportedExecutor::DryRun,
                ClientOrderId::from_uuid(Uuid::new_v4()),
                kind,
            ),
        )
        .await
        .unwrap()
    }

    // These tests install the process-global Prometheus recorder via
    // crate::metrics::setup() and assert on its rendered output. nextest (which
    // CI and local runs use) runs each test in its own process, so the
    // install-once recorder is fresh per test and the negative assertions below
    // are not contaminated by the sibling test's counter. The counters live in
    // the durable placement path (place_offchain_order_at_broker), so the tests
    // drive that path rather than the now-pure Place handler.

    #[tokio::test]
    async fn placement_increments_hedge_trades_counter_on_success() {
        let handle = crate::metrics::setup().expect("install Prometheus recorder");
        let placer = noop_order_placer();

        place_at_broker(placer.as_ref()).await;

        let rendered = handle.render();
        assert!(
            rendered.contains("hedge_trades_total{") && rendered.contains("direction=\"buy\""),
            "a successful placement must increment hedge_trades_total{{direction=buy}}, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("broker_errors_total{"),
            "a successful placement must not touch broker_errors_total, got:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn place_extended_hours_limit_transitions_to_submitted() {
        let placer = noop_order_placer();
        let order = place_at_broker_with_kind(
            placer.as_ref(),
            CounterTradeOrderKind::ExtendedHoursLimit {
                limit_price: Positive::new(Usd::new(float!(195.25))).unwrap(),
            },
        )
        .await;

        assert!(
            matches!(
                order,
                Some(OffchainOrder::Submitted {
                    ref executor_order_id,
                    market_session: MarketSession::Extended,
                    ..
                }) if executor_order_id == &ExecutorOrderId::new("noop-limit")
            ),
            "expected Submitted extended-hours limit order, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn placement_increments_broker_errors_counter_on_failure() {
        let handle = crate::metrics::setup().expect("install Prometheus recorder");
        let placer = failing_order_placer();

        place_at_broker(placer.as_ref()).await;

        let rendered = handle.render();
        assert!(
            rendered.contains("broker_errors_total{")
                && rendered.contains("kind=\"place_order_failed\""),
            "a failed placement must increment broker_errors_total{{kind=place_order_failed}}, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("hedge_trades_total{"),
            "a failed placement must not increment hedge_trades_total, got:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn place_extended_hours_limit_failure_transitions_to_failed() {
        let placer = limit_failing_order_placer();
        let order = place_at_broker_with_kind(
            placer.as_ref(),
            CounterTradeOrderKind::ExtendedHoursLimit {
                limit_price: Positive::new(Usd::new(float!(195.25))).unwrap(),
            },
        )
        .await;

        assert!(
            matches!(&order, Some(OffchainOrder::Failed { error, .. }) if error.contains("Limit order rejected")),
            "expected Failed with limit-order error, got: {order:?}"
        );
    }

    fn overfilling_order_placer() -> Arc<dyn OrderPlacer> {
        struct Overfill;

        #[async_trait]
        impl OrderPlacer for Overfill {
            async fn place_market_order(
                &self,
                order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                let original = order.shares.inner().inner();
                let extra = st0x_float_macro::float!(1);
                let overfilled = (original + extra).expect("addition should not fail");

                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("OVERFILL"),
                    placed_shares: Positive::new(FractionalShares::new(overfilled)).unwrap(),
                    is_extended_hours: false,
                    limit_price: None,
                })
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                unimplemented!("test stub")
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                Ok(CancellationOutcome::Requested)
            }
        }

        Arc::new(Overfill)
    }

    #[tokio::test]
    async fn place_at_broker_records_broker_accepted_shares() {
        let placer = noop_order_placer();
        let order = place_at_broker(placer.as_ref()).await;

        let Some(OffchainOrder::Submitted { shares, .. }) = order else {
            panic!("expected Submitted, got {order:?}");
        };
        assert_eq!(
            shares,
            noop_placed_shares(Positive::new(FractionalShares::new(float!(100))).unwrap()),
            "persisted shares should reflect the broker-accepted quantity, not the request"
        );
    }

    #[tokio::test]
    async fn place_at_broker_with_failing_broker_marks_failed() {
        let placer = failing_order_placer();
        let order = place_at_broker(placer.as_ref()).await;

        assert!(
            matches!(
                &order,
                Some(OffchainOrder::Failed { error, .. }) if error.contains("Broker rejected")
            ),
            "expected Failed with the broker error, got {order:?}"
        );
    }

    #[tokio::test]
    async fn place_at_broker_with_overfill_records_acceptance() {
        let placer = overfilling_order_placer();
        let order = place_at_broker(placer.as_ref()).await;

        // Per ADR 0009 a broker over-fill is recorded as an acceptance carrying
        // the actual broker-placed quantity, not a failure -- the live order must
        // stay poll-able instead of being failed and re-driven into a loop.
        let Some(OffchainOrder::Submitted {
            shares,
            executor_order_id,
            ..
        }) = order
        else {
            panic!("expected Submitted for an over-filling broker, got {order:?}");
        };
        let expected = Positive::new(FractionalShares::new(float!(101))).unwrap();
        assert_eq!(
            shares, expected,
            "over-fill should record the broker-placed quantity (requested 100 + 1)"
        );
        assert_eq!(
            executor_order_id,
            ExecutorOrderId::new("OVERFILL"),
            "over-fill must preserve the broker order id so the live order stays poll-able"
        );
    }

    #[tokio::test]
    async fn place_records_adopted_extended_hours_terms_over_requested_kind() {
        // Lost-response convergence scenario: the command asks for a MARKET
        // order (regular-open retry), but the placer adopts the prior
        // attempt's still-live extended-hours limit order via the broker's
        // duplicate-client_order_id reconciliation. The aggregate must record
        // the ADOPTED order's terms -- is_extended_hours=true and its limit
        // price -- or the regular-open cancel-and-replace sweep (keyed off
        // is_extended_hours) never converges the stale limit order.
        fn adopting_order_placer() -> Arc<dyn OrderPlacer> {
            struct Adopting;

            #[async_trait]
            impl OrderPlacer for Adopting {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ADOPTED_EXT_LIMIT"),
                        placed_shares: order.shares,
                        is_extended_hours: true,
                        limit_price: Some(Positive::new(Usd::new(float!(195.25))).unwrap()),
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!("test stub")
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(CancellationOutcome::Requested)
                }
            }

            Arc::new(Adopting)
        }

        let placer = adopting_order_placer();
        let order = place_at_broker(placer.as_ref())
            .await
            .expect("placement helper must produce an order");

        assert!(
            matches!(
                order,
                OffchainOrder::Submitted {
                    market_session: MarketSession::Extended,
                    ..
                }
            ),
            "adopted extended-hours terms must be recorded on the aggregate, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn place_retry_with_divergent_payload_is_rejected() {
        let pool = crate::test_utils::setup_test_db().await;
        let (store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(noop_order_placer())
            .await
            .unwrap();
        let id = OffchainOrderId::new();

        // First `Place` creates the Pending order.
        store.send(&id, place_command()).await.unwrap();

        // An exact replay (the durable path re-sending `Place`) is an idempotent
        // no-op and must not error.
        store.send(&id, place_command()).await.unwrap();

        // A retry whose identity fields diverge from the original is a caller bug,
        // not a replay: it must fail fast rather than silently no-op.
        let mismatched = OffchainOrderCommand::Place {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            kind: CounterTradeOrderKind::Market,
        };
        let error = store.send(&id, mismatched).await.unwrap_err();
        assert!(matches!(
            error,
            AggregateError::UserError(LifecycleError::Apply(
                OffchainOrderError::PlacePayloadMismatch
            ))
        ));
    }

    #[tokio::test]
    async fn place_at_broker_skips_broker_when_order_left_pending() {
        let pool = crate::test_utils::setup_test_db().await;
        let (store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(noop_order_placer())
            .await
            .unwrap();
        let id = OffchainOrderId::new();

        // At-least-once safety: a retry whose order already left `Pending` must
        // skip the broker call entirely (the broker dedupes, but we should not
        // even reach it). Drive the order to Submitted directly on the store.
        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("SETUP"),
                    placed_shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
                    submitted_at: Utc::now(),
                    market_session: MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();

        struct PanicIfCalled;

        #[async_trait]
        impl OrderPlacer for PanicIfCalled {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("broker must not be called for an order that already left Pending");
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("broker must not be called for an order that already left Pending");
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                Ok(CancellationOutcome::Requested)
            }
        }

        let result = place_offchain_order_at_broker(
            &store,
            &PanicIfCalled,
            &id,
            OffchainOrderPlacement::market(
                Symbol::new("AAPL").unwrap(),
                Positive::new(FractionalShares::new(float!(100))).unwrap(),
                Direction::Buy,
                SupportedExecutor::DryRun,
                ClientOrderId::from_uuid(Uuid::new_v4()),
            ),
        )
        .await
        .unwrap();

        assert!(
            matches!(result, Some(OffchainOrder::Submitted { .. })),
            "re-drive of an already-Submitted order must return it untouched, got {result:?}"
        );
    }

    #[tokio::test]
    async fn place_at_broker_skips_markfailed_when_order_advanced_concurrently() {
        let pool = crate::test_utils::setup_test_db().await;
        let (store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(noop_order_placer())
            .await
            .unwrap();
        let id = OffchainOrderId::new();

        // Concurrency narrowing: a concurrent attempt advances the order past
        // `Pending` (MarkAccepted) while this attempt's broker call is in flight
        // and then errors. The resulting `MarkPlacementFailed` must NOT clobber
        // the now-`Submitted` order -- the handler honours it only from `Pending`.
        struct AdvanceThenFail {
            store: Arc<st0x_event_sorcery::Store<OffchainOrder>>,
            id: OffchainOrderId,
        }

        #[async_trait]
        impl OrderPlacer for AdvanceThenFail {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                self.store
                    .send(
                        &self.id,
                        OffchainOrderCommand::MarkAccepted {
                            executor_order_id: ExecutorOrderId::new("CONCURRENT"),
                            placed_shares: Positive::new(FractionalShares::new(float!(100)))
                                .unwrap(),
                            submitted_at: Utc::now(),
                            market_session: MarketSession::Regular,
                            limit_price: None,
                        },
                    )
                    .await
                    .unwrap();

                Err("broker error after a concurrent attempt already succeeded".into())
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                unimplemented!("test stub")
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                Ok(CancellationOutcome::Requested)
            }
        }

        let placer = AdvanceThenFail {
            store: store.clone(),
            id,
        };

        let result = place_offchain_order_at_broker(
            &store,
            &placer,
            &id,
            OffchainOrderPlacement::market(
                Symbol::new("AAPL").unwrap(),
                Positive::new(FractionalShares::new(float!(100))).unwrap(),
                Direction::Buy,
                SupportedExecutor::DryRun,
                ClientOrderId::from_uuid(Uuid::new_v4()),
            ),
        )
        .await
        .unwrap();

        assert!(
            matches!(result, Some(OffchainOrder::Submitted { .. })),
            "a stale broker error must not clobber an order a concurrent attempt advanced, \
             got {result:?}"
        );
    }

    #[tokio::test]
    async fn place_is_idempotent_once_placed() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        // The durable placement path re-sends `Place` on retry; an existing
        // aggregate -- live or terminal -- must record nothing rather than error.
        store.send(&id, place_command()).await.unwrap();
        let pending = store.load(&id).await.unwrap().unwrap();
        store.send(&id, place_command()).await.unwrap();
        assert_eq!(
            pending,
            store.load(&id).await.unwrap().unwrap(),
            "re-placing a pending order is a no-op"
        );

        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "Market closed".to_string(),
                    filled_shares: None,
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        let failed = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(failed, OffchainOrder::Failed { .. }));

        store.send(&id, place_command()).await.unwrap();
        assert_eq!(
            failed,
            store.load(&id).await.unwrap().unwrap(),
            "re-placing a terminal order stays a no-op"
        );
    }

    /// Covers the fixture-only `PlaceAt` sibling of `Place`: it must thread
    /// the caller-supplied `placed_at` through to the emitted event's field
    /// rather than silently falling back to `Utc::now()`.
    #[tokio::test]
    async fn place_at_uses_supplied_timestamp() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        let placed_at = Utc::now() - chrono::Duration::hours(2);

        store
            .send(
                &id,
                OffchainOrderCommand::PlaceAt {
                    symbol: Symbol::new("AAPL").unwrap(),
                    shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
                    direction: Direction::Buy,
                    executor: SupportedExecutor::DryRun,
                    client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                    kind: CounterTradeOrderKind::Market,
                    placed_at,
                },
            )
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        let OffchainOrder::Pending {
            placed_at: stored_placed_at,
            ..
        } = entity
        else {
            panic!("Expected Pending state, got: {entity:?}");
        };
        assert_eq!(stored_placed_at, placed_at);
    }

    #[tokio::test]
    async fn mark_accepted_is_idempotent_on_submitted() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        // The durable placement path re-sends `MarkAccepted` on retry (the broker
        // dedupes the duplicate submission); a second acceptance on an already
        // -`Submitted` order must record nothing rather than overwrite it.
        place_and_submit(&store, &id).await;
        let submitted = store.load(&id).await.unwrap().unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("SECOND-ACCEPT"),
                    placed_shares: Positive::new(FractionalShares::new(float!(50))).unwrap(),
                    submitted_at: Utc::now(),
                    market_session: MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            submitted,
            store.load(&id).await.unwrap().unwrap(),
            "a duplicate MarkAccepted must not overwrite the working Submitted order"
        );
    }

    #[tokio::test]
    async fn mark_failed_is_idempotent_on_failed() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "first failure".to_string(),
                    filled_shares: None,
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        let failed = store.load(&id).await.unwrap().unwrap();

        // A retried placement whose broker call errored again must not overwrite
        // the recorded failure (which would churn the failed_at / error).
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "second failure".to_string(),
                    filled_shares: None,
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            failed,
            store.load(&id).await.unwrap().unwrap(),
            "a duplicate MarkFailed must leave the original failure untouched"
        );
    }

    #[tokio::test]
    async fn mark_accepted_after_failed_is_noop() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "broker rejected".to_string(),
                    filled_shares: None,
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        let failed = store.load(&id).await.unwrap().unwrap();

        // A late acceptance arriving after the order was already failed must not
        // resurrect it to Submitted.
        store
            .send(
                &id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("LATE-ACCEPT"),
                    placed_shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
                    submitted_at: Utc::now(),
                    market_session: MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            failed,
            store.load(&id).await.unwrap().unwrap(),
            "a MarkAccepted after Failed must not resurrect the order"
        );
    }

    #[tokio::test]
    async fn partial_fill_from_submitted_records_broker_timestamp() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        // A timestamp distinct from any wall clock the handler could stamp:
        // the persisted event must carry the broker's fill time, not
        // Utc::now() at processing time.
        let broker_partially_filled_at = Utc::now() - chrono::Duration::hours(3);

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(50)),
                    avg_price: Usd::new(float!(150.00)),
                    partially_filled_at: broker_partially_filled_at,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        let OffchainOrder::PartiallyFilled {
            partially_filled_at,
            ..
        } = inner
        else {
            panic!("expected PartiallyFilled, got {inner:?}");
        };
        assert_eq!(
            partially_filled_at, broker_partially_filled_at,
            "PartiallyFilled must persist the broker-reported fill time"
        );
    }

    #[tokio::test]
    async fn partial_fill_updates_shares() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        let latest_partially_filled_at = Utc::now();
        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(50)),
                    avg_price: Usd::new(float!(150.00)),
                    partially_filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(75)),
                    avg_price: Usd::new(float!(150.50)),
                    partially_filled_at: latest_partially_filled_at,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(50)),
                    avg_price: Usd::new(float!(151.00)),
                    partially_filled_at: latest_partially_filled_at + chrono::Duration::minutes(1),
                },
            )
            .await
            .unwrap();

        let OffchainOrder::PartiallyFilled {
            shares_filled,
            avg_price,
            partially_filled_at,
            ..
        } = store.load(&id).await.unwrap().unwrap()
        else {
            panic!("Expected PartiallyFilled state");
        };
        assert_eq!(shares_filled, FractionalShares::new(float!(75)));
        assert_eq!(avg_price, Usd::new(float!(150.50)));
        assert_eq!(partially_filled_at, latest_partially_filled_at);
    }

    #[tokio::test]
    async fn complete_fill_from_submitted() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.00)),
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(inner, OffchainOrder::Filled { .. }));
    }

    #[tokio::test]
    async fn complete_fill_from_partially_filled() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(75)),
                    avg_price: Usd::new(float!(150.00)),
                    partially_filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.25)),
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(inner, OffchainOrder::Filled { .. }));
    }

    #[tokio::test]
    async fn complete_fill_records_hedge_fill_latency_sample() {
        // Process-isolated under nextest; only this fill is sampled. The default
        // Prometheus summary rendering emits a `_count` series we can assert on.
        let handle = crate::metrics::setup().expect("install Prometheus recorder");
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.00)),
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let rendered = handle.render();
        assert!(
            rendered.contains("hedge_fill_latency_seconds_count{symbol=\"AAPL\"} 1"),
            "completing a fill must record exactly one hedge_fill_latency_seconds sample, got:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn cannot_fill_uninitialized_order() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.00)),
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(OffchainOrderError::NotPlaced))
        ));
    }

    #[tokio::test]
    async fn cannot_fill_already_filled() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.00)),
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.00)),
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(OffchainOrderError::AlreadyCompleted))
        ));
    }

    #[tokio::test]
    async fn mark_failed_from_submitted() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "Insufficient funds".to_string(),
                    filled_shares: None,
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(inner, OffchainOrder::Failed { .. }));
    }

    #[tokio::test]
    async fn mark_failed_from_partially_filled() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        let partially_filled_at = Utc::now();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(50)),
                    avg_price: Usd::new(float!(150.00)),
                    partially_filled_at,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "Order cancelled".to_string(),
                    filled_shares: Some(FractionalShares::new(float!(25))),
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                inner,
                OffchainOrder::Failed {
                    retained_fill:
                        Some(RetainedFill::Priced {
                            shares_filled,
                            partially_filled_at: fill_time,
                            ..
                        }),
                    filled_shares: Some(terminal_filled),
                    executor_order_id: Some(_),
                    ..
                } if shares_filled == FractionalShares::new(float!(50))
                    && fill_time == partially_filled_at
                    && terminal_filled == FractionalShares::new(float!(25))
            ),
            "A stale terminal failure must preserve the larger priced fill, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn larger_unpriced_failure_replaces_smaller_priced_fill() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(25)),
                    avg_price: Usd::new(float!(150.00)),
                    partially_filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "Order cancelled".to_string(),
                    filled_shares: Some(FractionalShares::new(float!(50))),
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                inner,
                OffchainOrder::Failed {
                    retained_fill: Some(RetainedFill::Unpriced { shares_filled }),
                    ..
                } if shares_filled == FractionalShares::new(float!(50))
            ),
            "A larger unpriced terminal fill must replace stale priced evidence, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn mark_placement_failed_fails_a_pending_order() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        // The placement path's broker call errored while the order was still
        // Pending, so the placement-initiated failure is recorded.
        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkPlacementFailed {
                    error: "broker unreachable".to_string(),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(matches!(inner, OffchainOrder::Failed { .. }));
    }

    #[tokio::test]
    async fn mark_placement_failed_leaves_a_live_order_untouched() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        // A stale placement attempt's broker error must never fail a live order a
        // concurrent attempt already drove past Pending: the handler enforces the
        // Pending-only narrowing against the aggregate's authoritative state, so a
        // MarkPlacementFailed on a Submitted order is a no-op (no version-gate
        // race, unlike the old caller-side load-then-send guard).
        place_and_submit(&store, &id).await;
        let submitted = store.load(&id).await.unwrap().unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::MarkPlacementFailed {
                    error: "stale broker error".to_string(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            submitted,
            store.load(&id).await.unwrap().unwrap(),
            "MarkPlacementFailed must leave a live Submitted order untouched"
        );
    }

    #[tokio::test]
    async fn cannot_fail_already_filled() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.00)),
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let err = store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "Test error".to_string(),
                    filled_shares: None,
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(OffchainOrderError::AlreadyCompleted))
        ));
    }

    #[tokio::test]
    async fn cancel_order_from_submitted_transitions_to_cancelling() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                inner,
                OffchainOrder::Cancelling {
                    reason: CancellationReason::MarketOpenReplacement,
                    ..
                }
            ),
            "Expected Cancelling with MarketOpenReplacement reason, got: {inner:?}"
        );
    }

    /// Builds a placer whose `get_order_status` reports the given broker
    /// partial fill, so cancel-path tests can exercise `reconcile_pre_cancel`
    /// against a locally recorded quantity.
    fn broker_partial_fill_placer(
        broker_shares_filled: rain_math_float::Float,
        broker_partially_filled_at: DateTime<Utc>,
    ) -> Arc<dyn OrderPlacer> {
        struct Placer {
            broker_shares_filled: rain_math_float::Float,
            broker_partially_filled_at: DateTime<Utc>,
        }

        #[async_trait]
        impl OrderPlacer for Placer {
            async fn place_market_order(
                &self,
                order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("ORD-OK"),
                    placed_shares: noop_placed_shares(order.shares),
                    is_extended_hours: false,
                    limit_price: None,
                })
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                unimplemented!()
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                Ok(CancellationOutcome::Requested)
            }

            async fn get_order_status(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::OrderState::PartiallyFilled {
                    order_id: ExecutorOrderId::new("ORD-OK"),
                    shares_filled: FractionalShares::new(self.broker_shares_filled),
                    avg_price: Some(Usd::new(st0x_float_macro::float!(155.0))),
                    partially_filled_at: self.broker_partially_filled_at,
                })
            }
        }

        Arc::new(Placer {
            broker_shares_filled,
            broker_partially_filled_at,
        })
    }

    /// At cancel time the broker reports MORE cumulative fills (60) than the
    /// local PartiallyFilled state records (50): `reconcile_pre_cancel` must
    /// apply the newer broker fill (with the broker's fill timestamp) before
    /// transitioning to Cancelling, otherwise the extra 10 shares are dropped
    /// and double-hedged by the next scan.
    #[tokio::test]
    async fn cancel_order_from_partially_filled_reconciles_newer_broker_fill() {
        let broker_partially_filled_at = Utc::now() - chrono::Duration::minutes(2);
        let store = TestStore::<OffchainOrder>::new(broker_partial_fill_placer(
            float!(60),
            broker_partially_filled_at,
        ));
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(50)),
                    avg_price: Usd::new(float!(150.0)),
                    partially_filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                inner,
                OffchainOrder::Cancelling {
                    reason: CancellationReason::MarketOpenReplacement,
                    retained_fill:
                        Some(RetainedFill::Priced {
                            shares_filled,
                            partially_filled_at: fill_time,
                            ..
                        }),
                    ..
                } if shares_filled == FractionalShares::new(float!(60))
                    && fill_time == broker_partially_filled_at
            ),
            "Expected Cancelling with the reconciled 60-share broker fill and \
             its broker timestamp, got: {inner:?}"
        );
    }

    /// At cancel time the broker reports FEWER cumulative fills (30) than the
    /// local PartiallyFilled state records (50) -- a stale broker read.
    /// `reconcile_pre_cancel` must not regress the recorded quantity: the
    /// order proceeds to Cancelling carrying the local 50-share fill.
    #[tokio::test]
    async fn cancel_order_from_partially_filled_skips_stale_broker_fill() {
        let store =
            TestStore::<OffchainOrder>::new(broker_partial_fill_placer(float!(30), Utc::now()));
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(50)),
                    avg_price: Usd::new(float!(150.0)),
                    partially_filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                inner,
                OffchainOrder::Cancelling {
                    reason: CancellationReason::MarketOpenReplacement,
                    retained_fill:
                        Some(RetainedFill::Priced {
                            shares_filled,
                            ..
                        }),
                    ..
                } if shares_filled == FractionalShares::new(float!(50))
            ),
            "Expected Cancelling preserving the local 50-share fill against \
             the stale 30-share broker read, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn confirm_cancellation_transitions_cancelling_to_cancelled() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::ConfirmCancellation {
                    filled_shares: FractionalShares::ZERO,
                    cancelled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                inner,
                OffchainOrder::Cancelled {
                    reason: CancellationReason::MarketOpenReplacement,
                    ..
                }
            ),
            "Expected confirmed cancellation to become terminal Cancelled, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn stale_cancellation_preserves_larger_priced_fill() {
        let partially_filled_at = Utc::now();
        let store = TestStore::<OffchainOrder>::new(broker_partial_fill_placer(
            float!(50),
            partially_filled_at,
        ));
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(50)),
                    avg_price: Usd::new(float!(150.0)),
                    partially_filled_at,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::ConfirmCancellation {
                    filled_shares: FractionalShares::new(float!(25)),
                    cancelled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                inner,
                OffchainOrder::Cancelled {
                    retained_fill:
                        Some(RetainedFill::Priced {
                            shares_filled,
                            partially_filled_at: fill_time,
                            ..
                        }),
                    filled_shares: Some(terminal_filled),
                    ..
                } if shares_filled == FractionalShares::new(float!(50))
                    && fill_time == partially_filled_at
                    && terminal_filled == FractionalShares::new(float!(25))
            ),
            "A stale terminal cancellation must preserve the larger priced fill, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_short_circuits_already_cancelled_zero_fill_without_avg_price() {
        fn already_cancelled_zero_fill_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-CANCELLED"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    panic!("cancel_order must not be called when broker already reports Cancelled");
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(st0x_execution::OrderState::Cancelled {
                        order_id: ExecutorOrderId::new("ORD-CANCELLED"),
                        cancelled_at: Utc::now(),
                        shares_filled: FractionalShares::ZERO,
                        avg_price: None,
                    })
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(already_cancelled_zero_fill_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(inner, OffchainOrder::Cancelled { .. }),
            "Zero-fill broker cancellation without avg_price must become terminal Cancelled, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_records_priced_fill_from_terminal_cancelled_status() {
        fn already_cancelled_priced_fill_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-CANCELLED-PRICED"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    panic!("cancel_order must not be called when broker already reports Cancelled");
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<OrderState, Box<dyn std::error::Error + Send + Sync>> {
                    Ok(OrderState::Cancelled {
                        order_id: ExecutorOrderId::new("ORD-CANCELLED-PRICED"),
                        cancelled_at: Utc::now(),
                        shares_filled: FractionalShares::new(float!(50)),
                        avg_price: Some(Usd::new(float!(150.0))),
                    })
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(already_cancelled_priced_fill_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                inner,
                OffchainOrder::Cancelled {
                    retained_fill:
                        Some(RetainedFill::Priced {
                            shares_filled,
                            avg_price,
                            ..
                        }),
                    ..
                } if shares_filled == FractionalShares::new(float!(50))
                    && avg_price == Usd::new(float!(150.0))
            ),
            "Terminal Cancelled status with a priced fill must retain that fill, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_propagates_broker_error_and_leaves_state_unchanged() {
        // Critical safety invariant: when the broker DELETE fails, the
        // aggregate MUST stay in Submitted so the caller can retry.
        // Emitting Cancelled on broker error would let a still-live broker
        // order coexist with a replacement, causing duplicate hedges.
        fn cancel_failing_placer() -> Arc<dyn OrderPlacer> {
            struct CancelFailing;

            #[async_trait]
            impl OrderPlacer for CancelFailing {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-OK"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    Err("simulated broker DELETE failure".into())
                }

                async fn get_order_status(
                    &self,
                    executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    // Order still live -> pre-cancel reconciliation is a no-op,
                    // so the flow reaches the failing DELETE under test.
                    Ok(st0x_execution::OrderState::Submitted {
                        order_id: executor_order_id.clone(),
                    })
                }
            }

            Arc::new(CancelFailing)
        }

        let store = TestStore::<OffchainOrder>::new(cancel_failing_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::CancelFailed { .. }
                ))
            ),
            "Expected CancelFailed, got: {err:?}"
        );

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(inner, OffchainOrder::Submitted { .. }),
            "Aggregate MUST stay Submitted on broker cancel failure, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_resolves_terminal_cancelled_when_broker_does_not_know_order() {
        // The DELETE can 404 if the broker purged the order between the
        // pre-cancel status read and the cancel. Entering Cancelling would
        // strand the order forever: the status poll also 404s (as an error,
        // never a cancellation confirmation) and CancelOrder on Cancelling is
        // a no-op. The aggregate must resolve terminally instead.
        fn order_not_found_placer() -> Arc<dyn OrderPlacer> {
            struct OrderGone;

            #[async_trait]
            impl OrderPlacer for OrderGone {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-GONE"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(CancellationOutcome::OrderNotFound)
                }

                async fn get_order_status(
                    &self,
                    executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    // Order still live at the pre-cancel read; it vanishes
                    // only when the DELETE runs.
                    Ok(st0x_execution::OrderState::Submitted {
                        order_id: executor_order_id.clone(),
                    })
                }
            }

            Arc::new(OrderGone)
        }

        let store = TestStore::<OffchainOrder>::new(order_not_found_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                inner,
                OffchainOrder::Cancelled {
                    reason: CancellationReason::MarketOpenReplacement,
                    ..
                }
            ),
            "Broker-side 404 on cancel must resolve to terminal Cancelled \
             (not strand the order in Cancelling), got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_on_pending_returns_not_submitted() {
        // A genuinely `Pending` order: placed locally but not yet acknowledged
        // by the broker. CancelOrder must reject (`NotSubmitted`) so we never
        // issue a DELETE for an id the broker never returned. Construct the
        // state directly because the command path (Place) always drives the
        // order to Submitted or Failed, never leaving it Pending.
        let pending = OffchainOrder::Pending {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::DryRun,
            placed_at: Utc::now(),
            market_session: MarketSession::Regular,
        };

        let err = pending
            .transition(
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
                &noop_order_placer(),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, OffchainOrderError::NotSubmitted),
            "Expected NotSubmitted from cancel on Pending, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_on_failed_returns_already_completed() {
        // Placement feedback drives the aggregate to Failed; cancel on a
        // terminal state must reject with AlreadyCompleted.
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        store.send(&id, place_command()).await.unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkPlacementFailed {
                    error: "broker rejected".to_string(),
                },
            )
            .await
            .unwrap();

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::AlreadyCompleted
                ))
            ),
            "Expected AlreadyCompleted from cancel on Failed, got: {err:?}"
        );
    }

    /// Events persisted before this PR lack the new `Placed` fields
    /// (`is_extended_hours` / `limit_price` / `client_order_id`). Replaying
    /// such a legacy stream through the real `EventSourced` machinery -- the
    /// path startup recovery takes for orders that predate the schema change
    /// -- must still produce the correct terminal `Failed` aggregate with no
    /// fabricated fill data.
    #[test]
    fn legacy_event_stream_replays_to_failed_without_fill_data() {
        let failed_at = Utc::now();
        let placed = OffchainOrderEvent::Placed {
            symbol: Symbol::new("TSLA").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            placed_at: Utc::now(),
            is_extended_hours: false,
            limit_price: None,
            client_order_id: None,
        };

        // Strip the post-upgrade keys to reconstruct the exact payload shape
        // a pre-upgrade deployment persisted.
        let mut placed_value = serde_json::to_value(&placed).unwrap();
        let placed_object = placed_value
            .get_mut("Placed")
            .and_then(serde_json::Value::as_object_mut)
            .expect("Placed variant serializes as an object");
        placed_object.remove("is_extended_hours");
        placed_object.remove("limit_price");
        placed_object.remove("client_order_id");

        let mut failed_value = serde_json::to_value(OffchainOrderEvent::Failed {
            error: "broker rejected".to_string(),
            filled_shares: None,
            failed_at,
        })
        .unwrap();
        failed_value["Failed"]
            .as_object_mut()
            .unwrap()
            .remove("filled_shares");

        let legacy_events: Vec<OffchainOrderEvent> = [
            placed_value,
            serde_json::to_value(OffchainOrderEvent::Submitted {
                executor_order_id: ExecutorOrderId::new("broker-123"),
                submitted_at: Utc::now(),
            })
            .unwrap(),
            failed_value,
        ]
        .into_iter()
        .map(|value| serde_json::from_value(value).unwrap())
        .collect();

        let replayed = replay::<OffchainOrder>(legacy_events)
            .unwrap()
            .expect("legacy stream must replay to a live aggregate");

        let OffchainOrder::Failed {
            symbol,
            retained_fill,
            executor_order_id,
            error,
            failed_at: replayed_failed_at,
            ..
        } = replayed
        else {
            panic!("expected Failed, got {replayed:?}");
        };
        assert_eq!(symbol, Symbol::new("TSLA").unwrap());
        // No partial fill was ever recorded, so recovery must see no fill to
        // apply -- fabricating one here would corrupt the position.
        assert_eq!(retained_fill, None);
        assert_eq!(
            executor_order_id,
            Some(ExecutorOrderId::new("broker-123")),
            "Failed must retain the broker order id from the Submitted event"
        );
        assert_eq!(error, "broker rejected");
        assert_eq!(replayed_failed_at, failed_at);
    }

    #[test]
    fn legacy_event_stream_replays_to_cancelled_without_fill_data() {
        let placed_at = Utc::now();
        let cancelled_at = Utc::now();
        let mut cancelled_value = serde_json::to_value(OffchainOrderEvent::Cancelled {
            reason: CancellationReason::Unrequested,
            filled_shares: None,
            cancelled_at,
        })
        .unwrap();
        cancelled_value["Cancelled"]
            .as_object_mut()
            .unwrap()
            .remove("filled_shares");
        let events = vec![
            OffchainOrderEvent::Placed {
                symbol: Symbol::new("TSLA").unwrap(),
                shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                placed_at,
                is_extended_hours: false,
                limit_price: None,
                client_order_id: None,
            },
            OffchainOrderEvent::Submitted {
                executor_order_id: ExecutorOrderId::new("broker-cancelled"),
                submitted_at: placed_at,
            },
            serde_json::from_value(cancelled_value).unwrap(),
        ];

        let replayed = replay::<OffchainOrder>(events).unwrap().unwrap();
        assert!(matches!(
            replayed,
            OffchainOrder::Cancelled {
                filled_shares: None,
                retained_fill: None,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn cancel_order_on_filled_returns_already_completed() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.0)),
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            AggregateError::UserError(LifecycleError::Apply(OffchainOrderError::AlreadyCompleted))
        ));
    }

    #[tokio::test]
    async fn cancel_order_reconciles_partial_fill_before_cancelling() {
        // Critical correctness: if the broker reports a partial fill that
        // the local aggregate doesn't know about yet, CancelOrder must
        // emit PartiallyFilled BEFORE Cancelled, otherwise the partial
        // fill is silently dropped and the position double-hedges.
        fn partial_fill_reconciling_placer() -> Arc<dyn OrderPlacer> {
            struct PartialFillPlacer;

            #[async_trait]
            impl OrderPlacer for PartialFillPlacer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-OK"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(CancellationOutcome::Requested)
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    // Broker reports 50 shares filled at $150 -- a partial
                    // fill the local aggregate doesn't know about.
                    Ok(st0x_execution::OrderState::PartiallyFilled {
                        order_id: ExecutorOrderId::new("ORD-OK"),
                        shares_filled: FractionalShares::new(float!(50)),
                        avg_price: Some(Usd::new(float!(150.0))),
                        partially_filled_at: Utc::now(),
                    })
                }
            }

            Arc::new(PartialFillPlacer)
        }

        let store = TestStore::<OffchainOrder>::new(partial_fill_reconciling_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        // Must be Cancelling, but the partial-fill event should have been
        // emitted en route so the eventual terminal cancellation can
        // finalize the partial fill correctly.
        assert!(
            matches!(
                inner,
                OffchainOrder::Cancelling {
                    retained_fill:
                        Some(RetainedFill::Priced {
                            shares_filled,
                            ..
                        }),
                    ..
                } if shares_filled == FractionalShares::new(float!(50))
            ),
            "Expected Cancelling with reconciled partial fill, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_blocks_when_broker_failed_with_unpriced_fill() {
        // If the broker reports the order Failed with filled shares but no
        // avg_price at cancel time, CancelOrder must NOT emit a bare Failed
        // (which would clear the position via FailOffChainOrder and silently
        // drop the filled shares -> the next scan double-hedges them). It must
        // propagate PreCancelPartialFillMissingAvgPrice so the cancel is
        // retried once the broker returns a priced fill, leaving the aggregate
        // in its prior Submitted state. Mirrors the Cancelled-arm guard.
        fn failed_unpriced_fill_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-OK"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(CancellationOutcome::Requested)
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    // Broker reports a Failed order carrying 50 filled shares
                    // but no avg_price -- we cannot price the fill.
                    Ok(st0x_execution::OrderState::Failed {
                        failed_at: Utc::now(),
                        error_reason: Some("broker failed".to_string()),
                        shares_filled: Some(FractionalShares::new(float!(50))),
                        avg_price: None,
                    })
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(failed_unpriced_fill_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::PreCancelPartialFillMissingAvgPrice { .. }
                ))
            ),
            "Expected PreCancelPartialFillMissingAvgPrice, got: {err:?}"
        );

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(inner, OffchainOrder::Submitted { .. }),
            "Aggregate MUST stay Submitted so the unpriced fill is not dropped, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_blocks_when_broker_cancelled_with_unpriced_fill() {
        // The Cancelled-arm twin of the Failed-arm guard above: a broker
        // Cancelled carrying a positive fill without an avg_price must block
        // (PreCancelPartialFillMissingAvgPrice) rather than emit a bare
        // Cancelled that clears the position and drops the filled shares.
        fn cancelled_unpriced_fill_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-OK"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    panic!("cancel_order must not be called when broker already reports Cancelled");
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    // Broker reports an already-Cancelled order carrying 50
                    // filled shares but no avg_price -- we cannot price the
                    // fill, so the cancellation must not finalize yet.
                    Ok(st0x_execution::OrderState::Cancelled {
                        order_id: ExecutorOrderId::new("ORD-OK"),
                        cancelled_at: Utc::now(),
                        shares_filled: FractionalShares::new(float!(50)),
                        avg_price: None,
                    })
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(cancelled_unpriced_fill_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::PreCancelPartialFillMissingAvgPrice { .. }
                ))
            ),
            "Expected PreCancelPartialFillMissingAvgPrice, got: {err:?}"
        );

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(inner, OffchainOrder::Submitted { .. }),
            "Aggregate MUST stay Submitted so the unpriced fill is not dropped, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_blocks_when_broker_partially_filled_with_unpriced_fill() {
        // The live-order twin of the terminal guards above: a broker
        // PartiallyFilled carrying a positive fill without an avg_price at
        // cancel time cannot be recorded, so the cancel must block with
        // PreCancelPartialFillMissingAvgPrice BEFORE any DELETE is issued --
        // proceeding would drop the fill when the order later cancels.
        fn partially_filled_unpriced_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-OK"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    panic!("cancel_order must not run when the unpriced fill cannot be recorded");
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<OrderState, Box<dyn std::error::Error + Send + Sync>> {
                    Ok(OrderState::PartiallyFilled {
                        order_id: ExecutorOrderId::new("ORD-OK"),
                        shares_filled: FractionalShares::new(float!(50)),
                        avg_price: None,
                        partially_filled_at: Utc::now(),
                    })
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(partially_filled_unpriced_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::PreCancelPartialFillMissingAvgPrice { .. }
                ))
            ),
            "Expected PreCancelPartialFillMissingAvgPrice, got: {err:?}"
        );

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(inner, OffchainOrder::Submitted { .. }),
            "Aggregate MUST stay Submitted so the unpriced fill is not dropped, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_proceeds_when_unpriced_terminal_fill_is_covered_locally() {
        // An unpriced broker terminal fill must NOT block the cancel when the
        // local aggregate already recorded an equal priced fill: the broker
        // report carries no new information, so the cancellation finalizes
        // with the locally retained fill instead of retrying forever.
        fn cancelled_covered_fill_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-OK"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    panic!("cancel_order must not be called when broker already reports Cancelled");
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<OrderState, Box<dyn std::error::Error + Send + Sync>> {
                    // Same 50 shares the local aggregate already holds priced,
                    // but the broker response omits the price.
                    Ok(OrderState::Cancelled {
                        order_id: ExecutorOrderId::new("ORD-OK"),
                        cancelled_at: Utc::now(),
                        shares_filled: FractionalShares::new(float!(50)),
                        avg_price: None,
                    })
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(cancelled_covered_fill_placer());
        let id = OffchainOrderId::new();
        let fill_time = Utc::now();
        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(50)),
                    avg_price: Usd::new(float!(195.25)),
                    partially_filled_at: fill_time,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        let OffchainOrder::Cancelled { retained_fill, .. } = inner else {
            panic!("expected the cancellation to finalize, got: {inner:?}");
        };
        assert_eq!(
            retained_fill,
            Some(RetainedFill::priced(
                FractionalShares::new(float!(50)),
                Usd::new(float!(195.25)),
                fill_time
            )),
            "the locally recorded priced fill must be retained"
        );
    }

    /// The fail-closed contract of `reconcile_pre_cancel`: when the pre-cancel
    /// broker read fails, CancelOrder must propagate
    /// `PreCancelStatusFetchFailed` and leave the aggregate untouched (no
    /// DELETE issued) so the cancel retries instead of proceeding blind and
    /// potentially dropping a fill the broker just reported. The float
    /// comparison inside the same function shares this contract via `?`
    /// propagation (`FillComparisonFailed`); comparisons over real `Float`
    /// values are total, so the status fetch is the injectable failure seam.
    #[tokio::test]
    async fn cancel_order_blocks_when_pre_cancel_status_fetch_fails() {
        fn status_fetch_failing_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-OK"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    panic!("cancel_order must not be called when the pre-cancel read failed");
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    Err("simulated status API outage".into())
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(status_fetch_failing_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::PreCancelStatusFetchFailed { .. }
                ))
            ),
            "Expected PreCancelStatusFetchFailed, got: {err:?}"
        );

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(inner, OffchainOrder::Submitted { .. }),
            "Aggregate MUST stay Submitted when the pre-cancel read fails, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_short_circuits_to_filled_if_broker_already_filled() {
        // If the order completes at the broker between our last poll and
        // the cancel attempt, we must emit Filled (not Cancelled) and
        // NOT call DELETE -- the order is already terminal at the broker.
        fn already_filled_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-OK"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    panic!("cancel_order must not be called when broker reports Filled");
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(st0x_execution::OrderState::Filled {
                        order_id: ExecutorOrderId::new("ORD-OK"),
                        price: Usd::new(float!(150.0)),
                        executed_at: Utc::now(),
                    })
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(already_filled_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(inner, OffchainOrder::Filled { .. }),
            "Cancel-then-broker-Filled must short-circuit to Filled, got: {inner:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_on_already_cancelling_is_idempotent_noop() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(inner, OffchainOrder::Cancelling { .. }),
            "Duplicate cancel requests should leave order Cancelling, got: {inner:?}"
        );
    }

    #[test]
    fn non_genesis_event_on_uninitialized_produces_error() {
        let event = OffchainOrderEvent::Submitted {
            executor_order_id: ExecutorOrderId::new("ORD123"),
            submitted_at: Utc::now(),
        };

        let error = replay::<OffchainOrder>(vec![event]).unwrap_err();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
    }

    #[test]
    fn usd_serializes_as_decimal_string() {
        let usd = Usd::new(float!(150.25));
        let json = serde_json::to_value(usd).unwrap();
        assert_eq!(json, serde_json::json!("150.25"));
    }

    #[test]
    fn usd_deserializes_from_string() {
        let usd: Usd = serde_json::from_value(serde_json::json!("150.25")).unwrap();
        assert_eq!(usd, Usd::new(float!(150.25)));
    }

    #[test]
    fn usd_deserializes_from_number() {
        let usd: Usd = serde_json::from_value(serde_json::json!(150.25)).unwrap();
        assert_eq!(usd, Usd::new(float!(150.25)));
    }

    #[test]
    fn usd_round_trips_through_json() {
        let original = Usd::new(float!(99.99));
        let json = serde_json::to_string(&original).unwrap();
        let parsed: Usd = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, original);
    }

    #[tokio::test]
    async fn offchain_order_view_status_maps_all_lifecycle_states() {
        let pool = crate::test_utils::setup_test_db().await;

        let cases: &[(&str, Option<&str>)] = &[
            (r#"{"Live":{"Pending":{}}}"#, Some("Pending")),
            (r#"{"Live":{"Submitted":{}}}"#, Some("Submitted")),
            (
                r#"{"Live":{"PartiallyFilled":{}}}"#,
                Some("PartiallyFilled"),
            ),
            (r#"{"Live":{"Cancelling":{}}}"#, Some("Cancelling")),
            (r#"{"Live":{"Filled":{}}}"#, Some("Filled")),
            (r#"{"Live":{"Failed":{}}}"#, Some("Failed")),
            (r#"{"Live":{"Cancelled":{}}}"#, Some("Cancelled")),
            // Unrecognized payload — CASE has no ELSE, so status column must be NULL.
            (r#"{"Live":{"UnknownFutureState":{}}}"#, None),
        ];

        for (index, (payload, expected_status)) in cases.iter().enumerate() {
            let view_id = format!("view-{index}");

            sqlx::query(
                "INSERT INTO offchain_order_view (view_id, version, payload) \
                 VALUES ($1, $2, $3)",
            )
            .bind(&view_id)
            .bind(1_i64)
            .bind(*payload)
            .execute(&pool)
            .await
            .unwrap();

            let status: Option<String> =
                sqlx::query_scalar("SELECT status FROM offchain_order_view WHERE view_id = $1")
                    .bind(&view_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();

            assert_eq!(
                status.as_deref(),
                *expected_status,
                "payload {payload} should produce status {expected_status:?}"
            );
        }
    }

    // FIX 3: ConfirmCancellation on a non-Cancelling state must return CancellationNotRequested.
    #[tokio::test]
    async fn confirm_cancellation_on_submitted_returns_cancellation_not_requested() {
        let store = TestStore::<OffchainOrder>::new(noop_order_placer());
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;

        let err = store
            .send(
                &id,
                OffchainOrderCommand::ConfirmCancellation {
                    filled_shares: FractionalShares::ZERO,
                    cancelled_at: Utc::now(),
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::CancellationNotRequested
                ))
            ),
            "ConfirmCancellation on Submitted order must return CancellationNotRequested, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_returns_invalid_terminal_fill_quantity_for_negative_broker_fill() {
        fn negative_fill_cancelled_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-NEG-FILL"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    panic!("cancel_order must not be called when broker already reports Cancelled");
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
                {
                    // Broker reports Cancelled with a negative fill quantity:
                    // a corrupt value that reconcile_terminal_fill must reject.
                    Ok(st0x_execution::OrderState::Cancelled {
                        order_id: ExecutorOrderId::new("ORD-NEG-FILL"),
                        cancelled_at: Utc::now(),
                        shares_filled: FractionalShares::new(float!(-1)),
                        avg_price: Some(Usd::new(float!(150.0))),
                    })
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(negative_fill_cancelled_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::InvalidTerminalFillQuantity { .. }
                ))
            ),
            "Negative broker fill quantity must return InvalidTerminalFillQuantity, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_rejects_negative_unpriced_terminal_fill() {
        fn negative_unpriced_cancelled_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-NEG-UNPRICED"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    panic!("cancel_order must not be called when broker already reports Cancelled");
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<OrderState, Box<dyn std::error::Error + Send + Sync>> {
                    Ok(OrderState::Cancelled {
                        order_id: ExecutorOrderId::new("ORD-NEG-UNPRICED"),
                        cancelled_at: Utc::now(),
                        shares_filled: FractionalShares::new(float!(-1)),
                        avg_price: None,
                    })
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(negative_unpriced_cancelled_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::InvalidTerminalFillQuantity { .. }
                ))
            ),
            "Negative unpriced broker fill must return InvalidTerminalFillQuantity, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn cancel_order_rejects_negative_pre_cancel_partial_fill() {
        fn negative_partial_fill_placer() -> Arc<dyn OrderPlacer> {
            struct Placer;

            #[async_trait]
            impl OrderPlacer for Placer {
                async fn place_market_order(
                    &self,
                    order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Ok(OrderPlacementResult {
                        executor_order_id: ExecutorOrderId::new("ORD-NEG-PARTIAL"),
                        placed_shares: noop_placed_shares(order.shares),
                        is_extended_hours: false,
                        limit_price: None,
                    })
                }

                async fn place_limit_order(
                    &self,
                    _order: LimitOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    unimplemented!()
                }

                async fn cancel_order(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
                {
                    panic!("cancel_order must not run after a corrupt pre-cancel fill");
                }

                async fn get_order_status(
                    &self,
                    _executor_order_id: &ExecutorOrderId,
                ) -> Result<OrderState, Box<dyn std::error::Error + Send + Sync>> {
                    Ok(OrderState::PartiallyFilled {
                        order_id: ExecutorOrderId::new("ORD-NEG-PARTIAL"),
                        shares_filled: FractionalShares::new(float!(-1)),
                        avg_price: Some(Usd::new(float!(150.0))),
                        partially_filled_at: Utc::now(),
                    })
                }
            }

            Arc::new(Placer)
        }

        let store = TestStore::<OffchainOrder>::new(negative_partial_fill_placer());
        let id = OffchainOrderId::new();
        place_and_submit(&store, &id).await;

        let err = store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::InvalidTerminalFillQuantity { .. }
                ))
            ),
            "Negative pre-cancel partial fill must return InvalidTerminalFillQuantity, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn mark_failed_on_cancelling_retains_partial_fill_metadata() {
        let partially_filled_at = Utc::now() - chrono::Duration::minutes(5);
        // Broker reports the same 50-share fill as the local state (stale read);
        // pre-cancel reconciliation skips the update and the order reaches
        // Cancelling carrying the locally-recorded fill.
        let store = TestStore::<OffchainOrder>::new(broker_partial_fill_placer(
            float!(50),
            partially_filled_at,
        ));
        let id = OffchainOrderId::new();

        place_and_submit(&store, &id).await;
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(50)),
                    avg_price: Usd::new(float!(150.0)),
                    partially_filled_at,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "Extended hours session expired".to_string(),
                    filled_shares: None,
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let inner = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                inner,
                OffchainOrder::Failed {
                    retained_fill:
                        Some(RetainedFill::Priced {
                            shares_filled,
                            ..
                        }),
                    ..
                } if shares_filled == FractionalShares::new(float!(50))
            ),
            "MarkFailed on Cancelling must retain partial-fill metadata, got: {inner:?}"
        );
    }
}
