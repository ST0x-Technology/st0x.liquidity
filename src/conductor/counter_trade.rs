//! Counter-trade submission policy and coordination.

use std::collections::HashMap;

use st0x_event_sorcery::{Projection, Store};
use tracing::{info, warn};

use st0x_execution::{
    CounterTradePreflight, CounterTradeReservation, CounterTradeSkipReason, ExecutionError,
    Executor, FractionalShares, MarketOrder, Symbol,
};

use crate::config::AssetsConfig;
use crate::offchain_order::{OffchainOrder, OffchainOrderCommand, OffchainOrderId};
use crate::onchain::accumulator::{ExecutionCtx, check_all_positions};
use crate::position::{Position, PositionCommand};
use crate::threshold::ExecutionThreshold;

use super::{EventProcessingError, TradeProcessingCqrs};

pub(crate) struct OffchainOrderViews<'a> {
    pub(crate) store: &'a Store<OffchainOrder>,
    pub(crate) counter_trade_submission_lock: &'a tokio::sync::Mutex<()>,
}

impl<'a> OffchainOrderViews<'a> {
    pub(crate) fn new(
        store: &'a Store<OffchainOrder>,
        _projection: &'a Projection<OffchainOrder>,
        counter_trade_submission_lock: &'a tokio::sync::Mutex<()>,
    ) -> Self {
        Self {
            store,
            counter_trade_submission_lock,
        }
    }
}

pub(super) async fn submit_ready_counter_trade<E: Executor>(
    executor: &E,
    execution: &ExecutionCtx,
    cqrs: &TradeProcessingCqrs,
) -> Result<Option<OffchainOrderId>, EventProcessingError>
where
    EventProcessingError: From<E::Error>,
{
    let _counter_trade_submission_guard = cqrs.counter_trade_submission_lock.lock().await;

    if !preflight_counter_trade_submission(executor, execution, None).await? {
        return Ok(None);
    }

    super::place_offchain_order(execution, cqrs).await
}

#[derive(Default)]
struct CounterTradeBatchBudget {
    reserved_buying_power_cents: i64,
    remaining_equity: HashMap<Symbol, FractionalShares>,
}

impl CounterTradeBatchBudget {
    fn reserve_buying_power(&mut self, estimated_cost_cents: i64) -> Result<(), ExecutionError> {
        self.reserved_buying_power_cents = self
            .reserved_buying_power_cents
            .checked_add(estimated_cost_cents)
            .ok_or(ExecutionError::BuyingPowerReservationOverflow {
                current_reserved_cents: self.reserved_buying_power_cents,
                additional_cents: estimated_cost_cents,
            })?;

        Ok(())
    }

    fn reserve(
        &mut self,
        reservation: &CounterTradeReservation,
    ) -> Result<Option<CounterTradeSkipReason>, ExecutionError> {
        match reservation {
            CounterTradeReservation::Equity {
                symbol,
                required,
                available,
            } => {
                let remaining = self
                    .remaining_equity
                    .entry(symbol.clone())
                    .or_insert(*available);

                if !remaining
                    .inner()
                    .gte(required.inner().inner())
                    .map_err(ExecutionError::from)?
                {
                    return Ok(Some(CounterTradeSkipReason::InsufficientEquity {
                        required: *required,
                        available: *remaining,
                    }));
                }

                *remaining = (*remaining - required.inner()).map_err(ExecutionError::from)?;
                Ok(None)
            }
            CounterTradeReservation::BuyingPower {
                estimated_cost_cents,
                available_buying_power_cents,
            } => {
                let remaining = available_buying_power_cents
                    .checked_sub(self.reserved_buying_power_cents)
                    .ok_or(ExecutionError::BuyingPowerReservationOverflow {
                        current_reserved_cents: self.reserved_buying_power_cents,
                        additional_cents: *available_buying_power_cents,
                    })?;

                if remaining < *estimated_cost_cents {
                    return Ok(Some(CounterTradeSkipReason::InsufficientBuyingPower {
                        estimated_cost_cents: *estimated_cost_cents,
                        available_buying_power_cents: remaining,
                    }));
                }

                self.reserve_buying_power(*estimated_cost_cents)?;
                Ok(None)
            }
        }
    }
}

fn log_counter_trade_skip(
    execution: &ExecutionCtx,
    source: &'static str,
    reason: &CounterTradeSkipReason,
) {
    match reason {
        CounterTradeSkipReason::InsufficientEquity {
            required,
            available,
        } => {
            warn!(
                symbol = %execution.symbol,
                shares = %execution.shares,
                direction = ?execution.direction,
                source,
                required_shares = %required,
                available_shares = %available,
                "Skipping counter trade before broker submission: insufficient offchain equity"
            );
        }
        CounterTradeSkipReason::InsufficientBuyingPower {
            estimated_cost_cents,
            available_buying_power_cents,
        } => {
            warn!(
                symbol = %execution.symbol,
                shares = %execution.shares,
                direction = ?execution.direction,
                source,
                estimated_cost_cents,
                available_buying_power_cents,
                "Skipping counter trade before broker submission: insufficient buying power"
            );
        }
    }
}

async fn preflight_counter_trade_submission<E: Executor>(
    executor: &E,
    execution: &ExecutionCtx,
    batch_budget: Option<&mut CounterTradeBatchBudget>,
) -> Result<bool, EventProcessingError>
where
    EventProcessingError: From<E::Error>,
{
    let order = MarketOrder {
        symbol: execution.symbol.clone(),
        shares: execution.shares,
        direction: execution.direction,
    };

    match executor.preflight_counter_trade(order).await? {
        CounterTradePreflight::Allowed { reservation } => {
            if let (Some(batch_budget), Some(reservation)) = (batch_budget, reservation.as_ref())
                && let Some(reason) = batch_budget.reserve(reservation)?
            {
                log_counter_trade_skip(execution, "reservation_budget", &reason);
                return Ok(false);
            }

            Ok(true)
        }
        CounterTradePreflight::Skipped(reason) => {
            log_counter_trade_skip(execution, "broker_preflight", &reason);
            Ok(false)
        }
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
pub(crate) async fn check_and_execute_accumulated_positions<E>(
    executor: &E,
    position: &Store<Position>,
    position_projection: &Projection<Position>,
    offchain_orders: OffchainOrderViews<'_>,
    threshold: &ExecutionThreshold,
    assets: &AssetsConfig,
    is_trading_enabled: impl Fn(&Symbol) -> bool,
) -> Result<(), EventProcessingError>
where
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
{
    let _counter_trade_submission_guard =
        offchain_orders.counter_trade_submission_lock.lock().await;
    let executor_type = executor.to_supported_executor();
    let ready_positions = check_all_positions(
        executor,
        position_projection,
        executor_type,
        assets,
        is_trading_enabled,
    )
    .await?;

    if ready_positions.is_empty() {
        return Ok(());
    }

    info!(
        ready_positions = ready_positions.len(),
        "Found accumulated positions ready for execution"
    );

    let mut batch_budget = CounterTradeBatchBudget::default();

    for execution in ready_positions {
        if !preflight_counter_trade_submission(executor, &execution, Some(&mut batch_budget))
            .await?
        {
            continue;
        }

        let offchain_order_id = OffchainOrderId::new();

        info!(
            symbol = %execution.symbol,
            shares = %execution.shares,
            direction = ?execution.direction,
            %offchain_order_id,
            "Executing accumulated position"
        );

        let command = PositionCommand::PlaceOffChainOrder {
            offchain_order_id,
            shares: execution.shares,
            direction: execution.direction,
            executor: execution.executor,
            threshold: *threshold,
        };

        if let Err(error) = position.send(&execution.symbol, command).await {
            warn!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "Position::PlaceOffChainOrder rejected (likely pending execution), \
                 skipping OffchainOrder creation: {error}"
            );
            continue;
        }

        info!(
            %offchain_order_id,
            symbol = %execution.symbol,
            "Position marked as pending execution"
        );

        let command = OffchainOrderCommand::Place {
            symbol: execution.symbol.clone(),
            shares: execution.shares,
            direction: execution.direction,
            executor: execution.executor,
        };

        match offchain_orders
            .store
            .send(&offchain_order_id, command)
            .await
        {
            Ok(()) => info!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "Offchain order command processed for accumulated position"
            ),
            Err(error) => warn!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "Offchain order command failed for accumulated position: {error}"
            ),
        }

        match offchain_orders.store.load(&offchain_order_id).await {
            Ok(Some(OffchainOrder::Failed { error, .. })) => {
                warn!(
                    %offchain_order_id,
                    symbol = %execution.symbol,
                    %error,
                    "Broker rejected order, clearing position pending state"
                );
                super::execute_fail_offchain_order_position(
                    position,
                    offchain_order_id,
                    &execution,
                    error,
                )
                .await;
            }

            Ok(Some(OffchainOrder::Submitted { .. })) => {
                info!(
                    %offchain_order_id,
                    symbol = %execution.symbol,
                    "Order submitted to broker"
                );
            }

            Ok(Some(OffchainOrder::PartiallyFilled { .. })) => {
                info!(
                    %offchain_order_id,
                    symbol = %execution.symbol,
                    "Order partially filled by broker"
                );
            }

            Ok(Some(OffchainOrder::Filled { .. })) => {
                info!(
                    %offchain_order_id,
                    symbol = %execution.symbol,
                    "Order filled by broker"
                );
            }

            Ok(other) => {
                warn!(
                    %offchain_order_id,
                    symbol = %execution.symbol,
                    state = ?other,
                    "Unexpected offchain order state after accumulated placement"
                );
                super::execute_fail_offchain_order_position(
                    position,
                    offchain_order_id,
                    &execution,
                    format!("Unexpected offchain order state: {other:?}"),
                )
                .await;
            }

            Err(error) => {
                warn!(
                    %offchain_order_id,
                    symbol = %execution.symbol,
                    ?error,
                    "Failed to load offchain order after accumulated placement"
                );
                super::execute_fail_offchain_order_position(
                    position,
                    offchain_order_id,
                    &execution,
                    format!("Failed to load offchain order: {error}"),
                )
                .await;
            }
        }
    }

    Ok(())
}
