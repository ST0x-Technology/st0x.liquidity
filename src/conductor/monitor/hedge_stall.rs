//! Supervised hedge-stall monitor.
//!
//! [`HedgeStallMonitor`] alerts when counter-trading has gone quiet while it
//! should be working. The dead-letter and worker-failure alerts cover hedges
//! that fail loudly; this covers the pipeline that simply stops: an idle
//! `PlaceHedge` worker, a `CheckPositions` sweep that no longer reschedules, or
//! a broker order that never completes. It reads the `Position` projection and
//! the scan heartbeat directly, so it does not depend on the apalis workers it
//! watches.
//!
//! ## The tell
//!
//! Each poll observes the scan and each symbol, each with a progress marker.
//! A stall alerts once it has been observed on every poll for `stall_after`
//! with an unchanged marker:
//!
//! - **scan not running** (every session): the marker is the last completed
//!   full sweep, so it alerts when no sweep completes for `stall_after`.
//! - **per symbol**: the marker is the share total hedging has moved, which
//!   changes only when a hedge fills. The symbol is observed while one of
//!   these holds, and the alert names the one that holds now:
//!   - *order not completing*: the position has a pending order;
//!   - *anchored too long*: the position is ready to hedge but a failed-order
//!     anchor blocks it;
//!   - *exposure not placed*: the position is ready to hedge (threshold met,
//!     no pending order, no transfer reservation) and the last sweep did not
//!     hold it back on purpose.
//!
//!   One clock covers all three, so a symbol that alternates between them
//!   (an order rejected and re-placed, a limit order repriced again and again)
//!   still alerts when nothing fills.
//!
//! The per-symbol conditions count only while the session can hedge the
//! symbol (regular hours, or extended hours when enabled for it) and, for new
//! orders, while the trading schedule admits one. Outside that, the condition
//! is not observed and its clock resets, so a closed market or a flat book
//! never alerts, and a stall can alert no sooner than `stall_after` after the
//! open. A symbol with a placement dead-letter is left to that alert.
//!
//! When the session read fails, the last known session stands until its own
//! close time and then counts as closed: a broker outage during trading hours
//! still alerts, and one that starts at Friday's close cannot alert all
//! weekend.
//!
//! ## Alert de-duplication
//!
//! Same shape as the gas monitor: alert on the transition into the stalled
//! state, re-alert at most once per `realert_interval` while it lasts, and log
//! recovery at `info!` without notifying. The Grafana rule that delivers the
//! alert treats the repeated line as a state and sends its own resolved notice
//! once the lines stop.
//!
//! ## Alert text
//!
//! The downstream log classifier matches on the message text, so every alert
//! starts with [`ALERT_PREFIX`] and names its condition with a fixed phrase.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::sync::Mutex;
use tokio::time::MissedTickBehavior;
use tracing::{error, info, warn};

use st0x_config::{Ctx, HedgeStallCtx};
use st0x_event_sorcery::Projection;
use st0x_execution::{
    Executor, FractionalShares, MarketSession, MarketSessionStatus, Positive, Symbol,
};

use crate::alerts::Notifier;
use crate::position::{Position, PositionError};
use crate::position_check::{HedgeScanHeartbeat, backstop_sizing_assets};
use crate::trading::offchain::close_flatten::CloseFlattenPolicy;
use crate::trading::onchain::trade_accountant::DeadLetterReason;

const ALERT_PREFIX: &str = "Hedge stalled:";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum StallKind {
    ExposureNotPlaced,
    OrderNotCompleting,
    AnchoredTooLong,
}

impl StallKind {
    const fn phrase(self) -> &'static str {
        match self {
            Self::ExposureNotPlaced => "exposure not placed",
            Self::OrderNotCompleting => "order not completing",
            Self::AnchoredTooLong => "anchored too long",
        }
    }
}

/// One clock per symbol, not per condition: a symbol that alternates between
/// conditions (an order rejected and re-placed, an extended-hours limit order
/// repriced again and again) is still not getting hedged.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum StallKey {
    Scan,
    Symbol(Symbol),
}

/// What must change for a condition to count as progress.
#[derive(Clone, Debug, PartialEq)]
enum Progress {
    /// The last completed full sweep; `None` before the first one.
    Scan(Option<DateTime<Utc>>),
    /// Net shares minus onchain fills: moves only when a hedge fills or an
    /// operator adjusts the position.
    HedgedShares(FractionalShares),
}

/// What one poll saw.
#[derive(Clone, Debug, PartialEq)]
enum StallObservation {
    Scan {
        last_completed_at: Option<DateTime<Utc>>,
    },
    Symbol {
        symbol: Symbol,
        kind: StallKind,
        net: FractionalShares,
        hedged: FractionalShares,
    },
}

impl StallObservation {
    fn key(&self) -> StallKey {
        match self {
            Self::Scan { .. } => StallKey::Scan,
            Self::Symbol { symbol, .. } => StallKey::Symbol(symbol.clone()),
        }
    }

    fn progress(&self) -> Progress {
        match self {
            Self::Scan { last_completed_at } => Progress::Scan(*last_completed_at),
            Self::Symbol { hedged, .. } => Progress::HedgedShares(*hedged),
        }
    }
}

/// How the sweep treats a symbol, resolved from config for one poll. `None`
/// in [`SymbolInput::policy`] means no hedged chain enables the symbol, so
/// the sweep never hedges it.
#[derive(Clone, Copy, Debug, PartialEq)]
struct SymbolPolicy {
    operational_limit: Option<Positive<FractionalShares>>,
    extended_hours: bool,
    allows_new_order: bool,
}

struct SymbolInput<'a> {
    position: &'a Position,
    policy: Option<SymbolPolicy>,
}

/// Everything one poll read, for [`classify`].
struct Snapshot<'a> {
    symbols: Vec<SymbolInput<'a>>,
    session: MarketSession,
    last_scan_at: Option<DateTime<Utc>>,
    held: &'a HashSet<Symbol>,
    dead_lettered: &'a HashSet<Symbol>,
}

const fn session_hedges(session: MarketSession, extended_hours: bool) -> bool {
    match session {
        MarketSession::Regular => true,
        MarketSession::Extended => extended_hours,
        MarketSession::Overnight | MarketSession::Closed => false,
    }
}

const fn session_label(session: MarketSession) -> &'static str {
    match session {
        MarketSession::Regular => "regular",
        MarketSession::Extended => "extended",
        MarketSession::Overnight => "overnight",
        MarketSession::Closed => "closed",
    }
}

fn session_closes_at(status: MarketSessionStatus) -> Option<DateTime<Utc>> {
    match status.session {
        MarketSession::Regular => status.regular_session_closes_at,
        MarketSession::Extended => status.extended_session_closes_at,
        MarketSession::Overnight | MarketSession::Closed => None,
    }
}

/// The session to evaluate when the broker read failed: the last known one
/// until its close time, closed after that or when its close is unknown.
fn fallback_session(last_known: Option<MarketSessionStatus>, now: DateTime<Utc>) -> MarketSession {
    last_known
        .filter(|status| session_closes_at(*status).is_some_and(|closes_at| now < closes_at))
        .map_or(MarketSession::Closed, MarketSessionStatus::session)
}

fn hedged_shares(position: &Position) -> Result<FractionalShares, PositionError> {
    Ok(((position.net - position.accumulated_long)? + position.accumulated_short)?)
}

fn classify(snapshot: &Snapshot<'_>) -> Vec<StallObservation> {
    let scan = StallObservation::Scan {
        last_completed_at: snapshot.last_scan_at,
    };

    std::iter::once(scan)
        .chain(
            snapshot
                .symbols
                .iter()
                .filter_map(|input| classify_symbol(snapshot, input)),
        )
        .collect()
}

fn classify_symbol(snapshot: &Snapshot<'_>, input: &SymbolInput<'_>) -> Option<StallObservation> {
    let position = input.position;
    let policy = input.policy?;

    if !session_hedges(snapshot.session, policy.extended_hours) {
        return None;
    }

    let kind = stall_kind(snapshot, position, policy)?;

    let hedged = hedged_shares(position)
        .inspect_err(|error| {
            warn!(
                target: "hedge",
                symbol = %position.symbol,
                %error,
                "Hedge-stall check could not compute hedged shares"
            );
        })
        .ok()?;

    Some(StallObservation::Symbol {
        symbol: position.symbol.clone(),
        kind,
        net: position.net,
        hedged,
    })
}

/// Which stall condition, if any, holds for a symbol in a session that can
/// hedge it.
fn stall_kind(
    snapshot: &Snapshot<'_>,
    position: &Position,
    policy: SymbolPolicy,
) -> Option<StallKind> {
    if position.pending_offchain_order_id.is_some() {
        return position
            .meets_execution_threshold()
            .inspect_err(|error| {
                warn!(
                    target: "hedge",
                    symbol = %position.symbol,
                    %error,
                    "Hedge-stall check could not evaluate pending-order exposure"
                );
            })
            .ok()?
            .then_some(StallKind::OrderNotCompleting);
    }

    let ready = position
        .is_ready_for_execution(policy.operational_limit)
        .inspect_err(|error| {
            warn!(
                target: "hedge",
                symbol = %position.symbol,
                %error,
                "Hedge-stall check could not evaluate position readiness"
            );
        })
        .ok()?;

    if ready.is_none() || !policy.allows_new_order {
        return None;
    }

    if position.last_failed_offchain_order_id.is_some() {
        return Some(StallKind::AnchoredTooLong);
    }

    if snapshot.held.contains(&position.symbol) || snapshot.dead_lettered.contains(&position.symbol)
    {
        return None;
    }

    Some(StallKind::ExposureNotPlaced)
}

/// Per-key de-dup state. A key with no entry is healthy.
#[derive(Clone, Debug, PartialEq)]
enum StallState {
    /// Observed since `since` without progress, not yet for `stall_after`.
    Pending {
        since: DateTime<Utc>,
        progress: Progress,
    },
    Stalled {
        since: DateTime<Utc>,
        progress: Progress,
        last_alerted: DateTime<Utc>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Quiet,
    Alert { since: DateTime<Utc> },
    Recovered { since: DateTime<Utc> },
}

fn elapsed_at_least(since: DateTime<Utc>, now: DateTime<Utc>, bound: Duration) -> bool {
    (now - since).to_std().is_ok_and(|elapsed| elapsed >= bound)
}

/// Pure de-dup transition for one key. `observed` is the key's progress
/// marker this poll, or `None` when the condition no longer holds.
fn evaluate(
    state: Option<StallState>,
    observed: Option<&Progress>,
    timings: HedgeStallCtx,
    now: DateTime<Utc>,
) -> (Option<StallState>, Outcome) {
    let pending_from_now = |progress: &Progress| StallState::Pending {
        since: now,
        progress: progress.clone(),
    };

    match (state, observed) {
        (None | Some(StallState::Pending { .. }), None) => (None, Outcome::Quiet),
        (Some(StallState::Stalled { since, .. }), None) => (None, Outcome::Recovered { since }),
        (None, Some(observed)) => (Some(pending_from_now(observed)), Outcome::Quiet),
        (Some(StallState::Pending { since, progress }), Some(observed)) => {
            if progress != *observed {
                (Some(pending_from_now(observed)), Outcome::Quiet)
            } else if elapsed_at_least(since, now, timings.stall_after) {
                (
                    Some(StallState::Stalled {
                        since,
                        progress,
                        last_alerted: now,
                    }),
                    Outcome::Alert { since },
                )
            } else {
                (
                    Some(StallState::Pending { since, progress }),
                    Outcome::Quiet,
                )
            }
        }
        (
            Some(StallState::Stalled {
                since,
                progress,
                last_alerted,
            }),
            Some(observed),
        ) => {
            if progress != *observed {
                (
                    Some(pending_from_now(observed)),
                    Outcome::Recovered { since },
                )
            } else if elapsed_at_least(last_alerted, now, timings.realert_interval) {
                (
                    Some(StallState::Stalled {
                        since,
                        progress,
                        last_alerted: now,
                    }),
                    Outcome::Alert { since },
                )
            } else {
                (
                    Some(StallState::Stalled {
                        since,
                        progress,
                        last_alerted,
                    }),
                    Outcome::Quiet,
                )
            }
        }
    }
}

fn stalled_minutes(since: DateTime<Utc>, now: DateTime<Utc>) -> i64 {
    (now - since).num_minutes()
}

fn alert_message(
    observation: &StallObservation,
    session: MarketSession,
    since: DateTime<Utc>,
    now: DateTime<Utc>,
) -> String {
    let minutes = stalled_minutes(since, now);
    match observation {
        StallObservation::Scan { .. } => format!("{ALERT_PREFIX} scan not running for {minutes}m"),
        StallObservation::Symbol {
            symbol, kind, net, ..
        } => {
            format!(
                "{ALERT_PREFIX} {phrase} for {symbol}, net {net} shares, {minutes}m \
                 (session {session})",
                phrase = kind.phrase(),
                session = session_label(session),
            )
        }
    }
}

/// Long-running supervised task that raises hedge-stall alerts.
#[derive(Clone)]
pub(crate) struct HedgeStallMonitor<E> {
    pub(crate) executor: E,
    pub(crate) position_projection: Arc<Projection<Position>>,
    pub(crate) heartbeat: Arc<HedgeScanHeartbeat>,
    pub(crate) alerted_dead_letters: Arc<Mutex<HashSet<(Symbol, DeadLetterReason)>>>,
    pub(crate) close_flatten_policy: CloseFlattenPolicy,
    pub(crate) ctx: Ctx,
    pub(crate) notifier: Arc<dyn Notifier>,
    pub(crate) timings: HedgeStallCtx,
}

/// State carried between polls.
#[derive(Debug, Default)]
struct MonitorState {
    stalls: HashMap<StallKey, StallState>,
    last_session: Option<MarketSessionStatus>,
}

impl<E> HedgeStallMonitor<E>
where
    E: Executor + Clone + Send + Sync + 'static,
{
    async fn session(&self, state: &mut MonitorState, now: DateTime<Utc>) -> MarketSession {
        match self.executor.market_session_status().await {
            Ok(status) => {
                state.last_session = Some(status);
                status.session
            }
            Err(error) => {
                let session = fallback_session(state.last_session, now);
                warn!(
                    target: "hedge",
                    %error,
                    fallback_session = session_label(session),
                    "Hedge-stall check could not read the market session"
                );
                session
            }
        }
    }

    fn symbol_policy(&self, symbol: &Symbol, now: DateTime<Utc>) -> Option<SymbolPolicy> {
        let assets = backstop_sizing_assets(&self.ctx.chains, symbol)?;
        Some(SymbolPolicy {
            operational_limit: assets.operational_limit(symbol),
            extended_hours: self.ctx.assets.is_extended_hours_enabled(symbol),
            allows_new_order: self.close_flatten_policy.allows_new_order(symbol, now),
        })
    }

    /// Placement dead-letters already paged; a residual-after-close alert is
    /// not a placement failure, so it does not stand in for this one.
    async fn placement_dead_letters(&self) -> HashSet<Symbol> {
        self.alerted_dead_letters
            .lock()
            .await
            .iter()
            .filter_map(|(symbol, reason)| match reason {
                DeadLetterReason::SymbolScoped(_) | DeadLetterReason::BackpressureExhausted => {
                    Some(symbol.clone())
                }
                DeadLetterReason::ResidualAfterClose => None,
            })
            .collect()
    }

    async fn poll_once(&self, state: &mut MonitorState, now: DateTime<Utc>) {
        let session = self.session(state, now).await;
        let last_scan = self.heartbeat.latest();

        let (observations, positions_loaded) = match self.position_projection.load_all().await {
            Ok(positions) => {
                let dead_lettered = self.placement_dead_letters().await;
                let held = last_scan
                    .as_ref()
                    .map(|scan| scan.held.clone())
                    .unwrap_or_default();

                let observations = classify(&Snapshot {
                    symbols: positions
                        .iter()
                        .map(|(symbol, position)| SymbolInput {
                            position,
                            policy: self.symbol_policy(symbol, now),
                        })
                        .collect(),
                    session,
                    last_scan_at: last_scan.as_ref().map(|scan| scan.completed_at),
                    held: &held,
                    dead_lettered: &dead_lettered,
                });
                (observations, true)
            }
            // The sweep reads the same projection, so a failing load is the
            // moment the scan check matters most: evaluate it alone and leave
            // every symbol's clock where it was.
            Err(error) => {
                warn!(
                    target: "hedge",
                    %error,
                    "Hedge-stall check could not load positions; checking the scan only"
                );
                let scan = StallObservation::Scan {
                    last_completed_at: last_scan.as_ref().map(|scan| scan.completed_at),
                };
                (vec![scan], false)
            }
        };

        let observations: HashMap<StallKey, StallObservation> = observations
            .into_iter()
            .map(|observation| (observation.key(), observation))
            .collect();

        let keys: HashSet<StallKey> = state
            .stalls
            .keys()
            .filter(|key| positions_loaded || **key == StallKey::Scan)
            .chain(observations.keys())
            .cloned()
            .collect();

        for key in keys {
            let observation = observations.get(&key);
            let (next, outcome) = evaluate(
                state.stalls.remove(&key),
                observation.map(StallObservation::progress).as_ref(),
                self.timings,
                now,
            );
            if let Some(next) = next {
                state.stalls.insert(key.clone(), next);
            }

            self.act_on_outcome(&key, outcome, observation, session, now)
                .await;
        }
    }

    async fn act_on_outcome(
        &self,
        key: &StallKey,
        outcome: Outcome,
        observation: Option<&StallObservation>,
        session: MarketSession,
        now: DateTime<Utc>,
    ) {
        match outcome {
            Outcome::Quiet => {}
            Outcome::Alert { since } => {
                let Some(observation) = observation else {
                    warn!(target: "hedge", ?key, "Hedge stall alerted without an observation");
                    return;
                };
                let message = alert_message(observation, session, since, now);
                error!(target: "hedge", ?key, %since, "{message}");
                if let Err(error) = self.notifier.notify(&message).await {
                    warn!(target: "hedge", ?error, "Failed to deliver hedge-stall alert");
                }
            }
            Outcome::Recovered { since } => {
                info!(
                    target: "hedge",
                    ?key,
                    stalled_minutes = stalled_minutes(since, now),
                    "Hedge stall cleared"
                );
            }
        }
    }
}

impl<E> SupervisedTask for HedgeStallMonitor<E>
where
    E: Executor + Clone + Send + Sync + 'static,
{
    async fn run(&mut self) -> TaskResult {
        info!(
            target: "hedge",
            poll_interval_secs = self.timings.poll_interval.as_secs(),
            stall_after_secs = self.timings.stall_after.as_secs(),
            "Hedge-stall monitor started"
        );

        let mut interval = tokio::time::interval(self.timings.poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut state = MonitorState::default();

        loop {
            interval.tick().await;
            self.poll_once(&mut state, Utc::now()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use alloy::primitives::{Address, TxHash};

    use st0x_config::{
        ChainEquityAsset, EquityHedgePolicy, ExecutionThreshold, HedgedEquities, HedgingAssets,
        OperationMode, create_test_ctx_with_order_owner,
    };
    use st0x_event_sorcery::StoreBuilder;
    use st0x_evm::Chain;
    use st0x_execution::{Direction, MockExecutor};
    use st0x_float_macro::float;

    use super::*;
    use crate::alerts::{CapturingNotifier, LogNotifier};
    use crate::offchain::order::OffchainOrderId;
    use crate::position::{
        EquityTransferReservation, EquityTransferReservationId, EquityTransferReservationStatus,
        PositionCommand, TradeId,
    };
    use crate::position_check::CompletedScan;
    use crate::test_utils::setup_test_db;
    use crate::trading::onchain::trade_accountant::SymbolScopedReason;

    const TIMINGS: HedgeStallCtx = HedgeStallCtx {
        poll_interval: Duration::from_secs(60),
        stall_after: Duration::from_secs(900),
        realert_interval: Duration::from_secs(3600),
    };

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + seconds, 0).unwrap()
    }

    fn aapl() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    /// A position whose whole net came from onchain fills: nothing hedged yet.
    fn position(net: FractionalShares) -> Position {
        Position {
            symbol: aapl(),
            net,
            accumulated_long: net,
            accumulated_short: FractionalShares::ZERO,
            pending_offchain_order_id: None,
            equity_transfer_reservation: None,
            last_failed_offchain_order_id: None,
            last_acknowledged_trade_id: None,
            pending_acknowledged_trade_ids: BTreeSet::new(),
            threshold: ExecutionThreshold::whole_share(),
            last_updated: None,
            last_price: None,
        }
    }

    const OPEN: SymbolPolicy = SymbolPolicy {
        operational_limit: None,
        extended_hours: false,
        allows_new_order: true,
    };

    fn classify_one(
        position: &Position,
        policy: Option<SymbolPolicy>,
        session: MarketSession,
        held: &HashSet<Symbol>,
        dead_lettered: &HashSet<Symbol>,
    ) -> Vec<StallObservation> {
        classify(&Snapshot {
            symbols: vec![SymbolInput { position, policy }],
            session,
            last_scan_at: Some(at(0)),
            held,
            dead_lettered,
        })
        .into_iter()
        .filter(|observation| observation.key() != StallKey::Scan)
        .collect()
    }

    fn kinds(observations: &[StallObservation]) -> Vec<StallKind> {
        observations
            .iter()
            .filter_map(|observation| match observation {
                StallObservation::Symbol { kind, .. } => Some(*kind),
                StallObservation::Scan { .. } => None,
            })
            .collect()
    }

    fn classify_kinds(
        position: &Position,
        policy: SymbolPolicy,
        session: MarketSession,
    ) -> Vec<StallKind> {
        kinds(&classify_one(
            position,
            Some(policy),
            session,
            &HashSet::new(),
            &HashSet::new(),
        ))
    }

    #[test]
    fn ready_exposure_in_regular_hours_is_observed_as_not_placed() {
        let observations = classify_one(
            &position(FractionalShares::new(float!(5))),
            Some(OPEN),
            MarketSession::Regular,
            &HashSet::new(),
            &HashSet::new(),
        );

        assert_eq!(
            observations,
            vec![StallObservation::Symbol {
                symbol: aapl(),
                kind: StallKind::ExposureNotPlaced,
                net: FractionalShares::new(float!(5)),
                hedged: FractionalShares::ZERO,
            }]
        );
    }

    #[test]
    fn closed_and_overnight_sessions_observe_nothing() {
        for session in [MarketSession::Closed, MarketSession::Overnight] {
            assert_eq!(
                classify_kinds(&position(FractionalShares::new(float!(5))), OPEN, session),
                Vec::new(),
                "{session:?}"
            );
        }
    }

    #[test]
    fn extended_session_counts_only_with_extended_hours_enabled() {
        let exposed = position(FractionalShares::new(float!(5)));

        assert_eq!(
            classify_kinds(&exposed, OPEN, MarketSession::Extended),
            Vec::new()
        );
        assert_eq!(
            classify_kinds(
                &exposed,
                SymbolPolicy {
                    extended_hours: true,
                    ..OPEN
                },
                MarketSession::Extended
            ),
            vec![StallKind::ExposureNotPlaced]
        );
    }

    #[test]
    fn exposure_below_threshold_is_not_observed() {
        for net in [FractionalShares::ZERO, FractionalShares::new(float!(0.5))] {
            assert_eq!(
                classify_kinds(&position(net), OPEN, MarketSession::Regular),
                Vec::new()
            );
        }
    }

    #[test]
    fn symbol_no_hedged_chain_enables_is_not_observed() {
        assert_eq!(
            kinds(&classify_one(
                &position(FractionalShares::new(float!(5))),
                None,
                MarketSession::Regular,
                &HashSet::new(),
                &HashSet::new(),
            )),
            Vec::new()
        );
    }

    #[test]
    fn exposure_held_by_the_last_sweep_is_not_observed() {
        assert_eq!(
            kinds(&classify_one(
                &position(FractionalShares::new(float!(5))),
                Some(OPEN),
                MarketSession::Regular,
                &HashSet::from([aapl()]),
                &HashSet::new(),
            )),
            Vec::new()
        );
    }

    #[test]
    fn exposure_already_dead_lettered_is_not_observed() {
        assert_eq!(
            kinds(&classify_one(
                &position(FractionalShares::new(float!(5))),
                Some(OPEN),
                MarketSession::Regular,
                &HashSet::new(),
                &HashSet::from([aapl()]),
            )),
            Vec::new()
        );
    }

    #[test]
    fn exposure_outside_the_trading_schedule_is_not_observed() {
        assert_eq!(
            classify_kinds(
                &position(FractionalShares::new(float!(5))),
                SymbolPolicy {
                    allows_new_order: false,
                    ..OPEN
                },
                MarketSession::Regular
            ),
            Vec::new()
        );
    }

    #[test]
    fn exposure_with_a_transfer_reservation_is_not_observed() {
        let reserved = Position {
            equity_transfer_reservation: Some(EquityTransferReservation {
                id: EquityTransferReservationId::generate(),
                status: EquityTransferReservationStatus::Reserved,
            }),
            ..position(FractionalShares::new(float!(5)))
        };

        assert_eq!(
            classify_kinds(&reserved, OPEN, MarketSession::Regular),
            Vec::new()
        );
    }

    #[test]
    fn pending_order_is_observed_as_not_completing() {
        let order_id = OffchainOrderId::new();
        let pending = Position {
            pending_offchain_order_id: Some(order_id),
            ..position(FractionalShares::new(float!(5)))
        };

        let observations = classify_one(
            &pending,
            Some(OPEN),
            MarketSession::Regular,
            &HashSet::new(),
            &HashSet::new(),
        );

        assert_eq!(kinds(&observations), vec![StallKind::OrderNotCompleting]);
    }

    #[test]
    fn pending_order_with_flat_or_below_threshold_exposure_is_not_observed() {
        for net in [FractionalShares::ZERO, FractionalShares::new(float!(0.5))] {
            let pending = Position {
                pending_offchain_order_id: Some(OffchainOrderId::new()),
                ..position(net)
            };

            assert_eq!(
                classify_kinds(&pending, OPEN, MarketSession::Regular),
                Vec::new()
            );
        }
    }

    #[test]
    fn anchored_exposure_is_observed_as_anchored_even_when_dead_lettered() {
        let anchor = OffchainOrderId::new();
        let anchored = Position {
            last_failed_offchain_order_id: Some(anchor),
            ..position(FractionalShares::new(float!(5)))
        };

        let observations = classify_one(
            &anchored,
            Some(OPEN),
            MarketSession::Regular,
            &HashSet::from([aapl()]),
            &HashSet::from([aapl()]),
        );

        assert_eq!(kinds(&observations), vec![StallKind::AnchoredTooLong]);
    }

    #[test]
    fn a_symbol_keeps_one_clock_while_it_alternates_between_conditions() {
        let exposed = position(FractionalShares::new(float!(5)));
        let pending = Position {
            pending_offchain_order_id: Some(OffchainOrderId::new()),
            ..exposed.clone()
        };
        let observe = |position: &Position| {
            classify_one(
                position,
                Some(OPEN),
                MarketSession::Regular,
                &HashSet::new(),
                &HashSet::new(),
            )
            .remove(0)
        };

        let (state, _) = evaluate(None, Some(&observe(&exposed).progress()), TIMINGS, at(0));
        let (state, outcome) =
            evaluate(state, Some(&observe(&pending).progress()), TIMINGS, at(450));
        assert_eq!(outcome, Outcome::Quiet);
        let (_, outcome) = evaluate(state, Some(&observe(&exposed).progress()), TIMINGS, at(900));

        assert_eq!(
            outcome,
            Outcome::Alert { since: at(0) },
            "an order placed and rejected again and again is still not hedging"
        );
    }

    #[test]
    fn scan_is_observed_in_every_session_with_its_last_completion() {
        for session in [
            MarketSession::Regular,
            MarketSession::Extended,
            MarketSession::Overnight,
            MarketSession::Closed,
        ] {
            let observations = classify(&Snapshot {
                symbols: Vec::new(),
                session,
                last_scan_at: None,
                held: &HashSet::new(),
                dead_lettered: &HashSet::new(),
            });

            assert_eq!(
                observations,
                vec![StallObservation::Scan {
                    last_completed_at: None
                }],
                "{session:?}"
            );
        }
    }

    #[test]
    fn failed_session_read_keeps_the_last_session_until_its_close() {
        let regular = MarketSessionStatus {
            regular_session_closes_at: Some(at(3600)),
            ..MarketSessionStatus::without_close_metadata(MarketSession::Regular)
        };

        assert_eq!(
            fallback_session(Some(regular), at(3599)),
            MarketSession::Regular
        );
        assert_eq!(
            fallback_session(Some(regular), at(3600)),
            MarketSession::Closed,
            "after Friday's close an outage must not look like an open market"
        );
        assert_eq!(
            fallback_session(
                Some(MarketSessionStatus::without_close_metadata(
                    MarketSession::Regular
                )),
                at(0)
            ),
            MarketSession::Closed,
            "an unknown close cannot keep the session open"
        );
        assert_eq!(fallback_session(None, at(0)), MarketSession::Closed);

        let extended = MarketSessionStatus {
            extended_session_closes_at: Some(at(7200)),
            ..MarketSessionStatus::without_close_metadata(MarketSession::Extended)
        };
        assert_eq!(
            fallback_session(Some(extended), at(7199)),
            MarketSession::Extended
        );
        assert_eq!(
            fallback_session(Some(extended), at(7200)),
            MarketSession::Closed
        );
    }

    fn scan_at(seconds: i64) -> Progress {
        Progress::Scan(Some(at(seconds)))
    }

    #[test]
    fn a_condition_alerts_only_after_stall_after_without_progress() {
        let progress = scan_at(0);

        let (state, outcome) = evaluate(None, Some(&progress), TIMINGS, at(0));
        assert_eq!(outcome, Outcome::Quiet);

        let (state, outcome) = evaluate(state, Some(&progress), TIMINGS, at(899));
        assert_eq!(outcome, Outcome::Quiet);

        let (state, outcome) = evaluate(state, Some(&progress), TIMINGS, at(900));
        assert_eq!(outcome, Outcome::Alert { since: at(0) });
        assert_eq!(
            state,
            Some(StallState::Stalled {
                since: at(0),
                progress,
                last_alerted: at(900),
            })
        );
    }

    #[test]
    fn progress_restarts_the_window() {
        let (state, _) = evaluate(None, Some(&scan_at(0)), TIMINGS, at(0));
        let (state, outcome) = evaluate(state, Some(&scan_at(600)), TIMINGS, at(600));
        assert_eq!(outcome, Outcome::Quiet);

        let (_, outcome) = evaluate(state, Some(&scan_at(600)), TIMINGS, at(1200));
        assert_eq!(
            outcome,
            Outcome::Quiet,
            "the window restarted at 600, so 1200 is inside it"
        );
    }

    #[test]
    fn a_stall_realerts_at_the_realert_interval() {
        let stalled = Some(StallState::Stalled {
            since: at(0),
            progress: scan_at(0),
            last_alerted: at(900),
        });

        let (state, outcome) = evaluate(stalled, Some(&scan_at(0)), TIMINGS, at(4499));
        assert_eq!(outcome, Outcome::Quiet);

        let (_, outcome) = evaluate(state, Some(&scan_at(0)), TIMINGS, at(4500));
        assert_eq!(outcome, Outcome::Alert { since: at(0) });
    }

    #[test]
    fn recovery_from_pending_is_quiet_and_from_stalled_is_reported() {
        let pending = Some(StallState::Pending {
            since: at(0),
            progress: scan_at(0),
        });
        assert_eq!(
            evaluate(pending, None, TIMINGS, at(60)),
            (None, Outcome::Quiet)
        );

        let stalled = StallState::Stalled {
            since: at(0),
            progress: scan_at(0),
            last_alerted: at(900),
        };
        assert_eq!(
            evaluate(Some(stalled.clone()), None, TIMINGS, at(960)),
            (None, Outcome::Recovered { since: at(0) })
        );
        assert_eq!(
            evaluate(Some(stalled), Some(&scan_at(950)), TIMINGS, at(960)),
            (
                Some(StallState::Pending {
                    since: at(960),
                    progress: scan_at(950),
                }),
                Outcome::Recovered { since: at(0) }
            )
        );
    }

    /// The downstream log classifier matches on this text, so a reword must
    /// fail here before it silently unroutes the alert.
    #[test]
    fn alert_messages_keep_their_fixed_phrases() {
        let symbol = |kind| StallObservation::Symbol {
            symbol: aapl(),
            kind,
            net: FractionalShares::new(float!(5)),
            hedged: FractionalShares::ZERO,
        };
        let scan = StallObservation::Scan {
            last_completed_at: None,
        };

        assert_eq!(
            alert_message(&scan, MarketSession::Closed, at(0), at(960)),
            "Hedge stalled: scan not running for 16m"
        );
        assert_eq!(
            alert_message(
                &symbol(StallKind::ExposureNotPlaced),
                MarketSession::Regular,
                at(0),
                at(900)
            ),
            "Hedge stalled: exposure not placed for AAPL, net 5 shares, 15m (session regular)"
        );
        assert_eq!(
            alert_message(
                &symbol(StallKind::OrderNotCompleting),
                MarketSession::Extended,
                at(0),
                at(900)
            ),
            "Hedge stalled: order not completing for AAPL, net 5 shares, 15m (session extended)"
        );
        assert_eq!(
            alert_message(
                &symbol(StallKind::AnchoredTooLong),
                MarketSession::Regular,
                at(0),
                at(900)
            ),
            "Hedge stalled: anchored too long for AAPL, net 5 shares, 15m (session regular)"
        );
    }

    fn aapl_ctx() -> Ctx {
        let symbol = aapl();
        let mut ctx = Ctx {
            assets: HedgingAssets {
                equities: HedgedEquities {
                    retired_symbols: Vec::new(),
                    symbols: HashMap::from([(
                        symbol.clone(),
                        EquityHedgePolicy {
                            extended_hours_counter_trading: OperationMode::Disabled,
                            hedge_floor_shares: None,
                        },
                    )]),
                },
                cash: None,
            },
            execution_threshold: ExecutionThreshold::whole_share(),
            ..create_test_ctx_with_order_owner(Address::ZERO)
        };
        ctx.chains.primary_mut().assets.equities.symbols = HashMap::from([(
            symbol,
            ChainEquityAsset {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
                target_share: None,
            },
        )]);
        ctx
    }

    async fn exposed_monitor(
        notifier: Arc<dyn Notifier>,
    ) -> (
        HedgeStallMonitor<MockExecutor>,
        Arc<Mutex<HashSet<(Symbol, DeadLetterReason)>>>,
    ) {
        let pool = setup_test_db().await;
        let (position, position_projection) =
            StoreBuilder::<Position>::new(pool).build(()).await.unwrap();
        position
            .send(
                &aapl(),
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: aapl(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        chain: Chain::Base,
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                    block_number: None,
                },
            )
            .await
            .unwrap();

        let alerted_dead_letters = Arc::new(Mutex::new(HashSet::new()));
        let monitor = HedgeStallMonitor {
            executor: MockExecutor::new().with_market_session(MarketSession::Regular),
            position_projection,
            heartbeat: Arc::new(HedgeScanHeartbeat::default()),
            alerted_dead_letters: alerted_dead_letters.clone(),
            close_flatten_policy: CloseFlattenPolicy::from_secs(300).unwrap(),
            ctx: aapl_ctx(),
            notifier,
            timings: TIMINGS,
        };

        (monitor, alerted_dead_letters)
    }

    fn sweep(monitor: &HedgeStallMonitor<MockExecutor>, seconds: i64) {
        monitor.heartbeat.record(CompletedScan {
            completed_at: at(seconds),
            held: HashSet::new(),
        });
    }

    #[tokio::test]
    async fn unplaced_exposure_alerts_once_after_the_window_then_goes_quiet_at_the_close() {
        let notifier = Arc::new(CapturingNotifier::default());
        let (mut monitor, _) = exposed_monitor(notifier.clone()).await;
        let mut state = MonitorState::default();

        for seconds in [0, 60, 899] {
            sweep(&monitor, seconds);
            monitor.poll_once(&mut state, at(seconds)).await;
        }
        assert_eq!(notifier.messages(), Vec::<String>::new());

        sweep(&monitor, 900);
        monitor.poll_once(&mut state, at(900)).await;
        assert_eq!(
            notifier.messages(),
            vec![
                "Hedge stalled: exposure not placed for AAPL, net 5 shares, 15m (session regular)"
                    .to_owned()
            ]
        );

        monitor.executor = MockExecutor::new().with_market_session(MarketSession::Closed);
        for seconds in [960, 4600] {
            sweep(&monitor, seconds);
            monitor.poll_once(&mut state, at(seconds)).await;
        }
        assert_eq!(
            notifier.messages().len(),
            1,
            "a closed market clears the stall without another alert"
        );
    }

    #[tokio::test]
    async fn placement_dead_letter_suppresses_but_residual_after_close_does_not() {
        for (reason, expected_alerts) in [
            (
                DeadLetterReason::SymbolScoped(SymbolScopedReason::PlacementPreflight),
                0,
            ),
            (DeadLetterReason::ResidualAfterClose, 1),
        ] {
            let notifier = Arc::new(CapturingNotifier::default());
            let (monitor, dead_letters) = exposed_monitor(notifier.clone()).await;
            dead_letters.lock().await.insert((aapl(), reason));
            let mut state = MonitorState::default();

            for seconds in [0, 900] {
                sweep(&monitor, seconds);
                monitor.poll_once(&mut state, at(seconds)).await;
            }

            assert_eq!(notifier.messages().len(), expected_alerts, "{reason:?}");
        }
    }

    #[tokio::test]
    async fn broker_outage_in_regular_hours_still_alerts_until_the_close() {
        let notifier = Arc::new(CapturingNotifier::default());
        let (mut monitor, _) = exposed_monitor(notifier.clone()).await;
        monitor.executor = MockExecutor::new()
            .with_market_session(MarketSession::Regular)
            .with_regular_session_closes_at(at(2000));
        let mut state = MonitorState::default();

        sweep(&monitor, 0);
        monitor.poll_once(&mut state, at(0)).await;

        monitor.executor = MockExecutor::with_failure("429 Too Many Requests");
        sweep(&monitor, 900);
        monitor.poll_once(&mut state, at(900)).await;
        assert_eq!(
            notifier.messages().len(),
            1,
            "the last known regular session stands until its close"
        );

        sweep(&monitor, 5000);
        monitor.poll_once(&mut state, at(5000)).await;
        assert_eq!(
            notifier.messages().len(),
            1,
            "past the close the outage counts as a closed market"
        );
    }

    #[tokio::test]
    async fn stopped_scan_alerts_even_with_the_market_closed() {
        let notifier = Arc::new(CapturingNotifier::default());
        let (mut monitor, _) = exposed_monitor(notifier.clone()).await;
        monitor.executor = MockExecutor::new().with_market_session(MarketSession::Closed);
        let mut state = MonitorState::default();

        sweep(&monitor, 0);
        monitor.poll_once(&mut state, at(0)).await;
        monitor.poll_once(&mut state, at(900)).await;

        assert_eq!(
            notifier.messages(),
            vec!["Hedge stalled: scan not running for 15m".to_owned()]
        );
    }

    #[tokio::test]
    async fn failed_position_load_still_checks_the_scan() {
        let notifier = Arc::new(CapturingNotifier::default());
        let (mut monitor, _) = exposed_monitor(notifier.clone()).await;
        let pool = setup_test_db().await;
        let (_, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        pool.close().await;
        monitor.position_projection = projection;
        let mut state = MonitorState::default();

        sweep(&monitor, 0);
        monitor.poll_once(&mut state, at(0)).await;
        monitor.poll_once(&mut state, at(900)).await;

        assert_eq!(
            notifier.messages(),
            vec!["Hedge stalled: scan not running for 15m".to_owned()]
        );
    }

    /// Delivery goes through the structured operational-alert log; the
    /// classifier downstream reads the message text.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn stall_alert_is_an_operational_alert_log_line() {
        let (monitor, _) = exposed_monitor(Arc::new(LogNotifier)).await;
        let mut state = MonitorState::default();

        for seconds in [0, 900] {
            sweep(&monitor, seconds);
            monitor.poll_once(&mut state, at(seconds)).await;
        }

        logs_assert(|lines| {
            let alerts = lines
                .iter()
                .filter(|line| {
                    line.contains("operational_alert")
                        && line.contains("alert=true")
                        && line.contains(
                            "Hedge stalled: exposure not placed for AAPL, net 5 shares, 15m \
                             (session regular)",
                        )
                })
                .count();
            if alerts == 1 {
                Ok(())
            } else {
                Err(format!("expected one operational_alert line, got {alerts}"))
            }
        });
    }
}
