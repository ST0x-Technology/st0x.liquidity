//! Broker-close coordination with durable conservative schedule boundaries.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use st0x_pricing_types::trading_state::{
    CalendarEvidence, EligibleSessions, SCHEMA_VERSION, Schedule, TradingInterval, TradingScope,
    TradingState,
};
use thiserror::Error;
use tracing::{debug, error, info, warn};

use st0x_config::{
    TradingScheduleConfig, TradingScheduleEnvironment, TradingScheduleMode, TradingScheduleScope,
};
use st0x_execution::{MarketSessionStatus, Symbol};

use crate::trading::offchain::close_flatten::CloseFlattenWindow;

mod monitor;
pub(crate) use monitor::TradingScheduleMonitor;

#[derive(Debug, Clone)]
pub(crate) struct TradingScheduleStore {
    config: TradingScheduleConfig,
    emergency_buffer: chrono::TimeDelta,
    pool: SqlitePool,
    latches: Arc<RwLock<BTreeMap<String, ScheduleLatch>>>,
    broker_windows: Arc<RwLock<BTreeMap<String, BrokerBoundary>>>,
    update_lock: Arc<tokio::sync::Mutex<()>>,
    changed: Arc<tokio::sync::Notify>,
}

#[derive(Debug, Clone, Copy)]
struct BrokerBoundary {
    window: CloseFlattenWindow,
    reached_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScheduleLatch {
    accepted: TradingState,
    interval: TradingInterval,
    conflicted: bool,
    #[serde(default)]
    broker_reopened: bool,
    #[serde(default)]
    reached_boundary: i64,
}

#[derive(Debug, Error)]
pub(crate) enum TradingScheduleError {
    #[error("invalid or expired trading schedule evidence")]
    Evidence,
    #[error(transparent)]
    ResponseEvidence(#[from] ResponseEvidenceError),
    #[error("trading schedule identity or membership mismatch")]
    Identity,
    #[error("trading schedule has no usable interval")]
    Unknown,
    #[error("trading schedule interval overlaps an existing latch")]
    Overlap,
    #[error("schedule latch lock poisoned")]
    Lock,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum ResponseEvidenceError {
    #[error("schedule observation is in the future")]
    FutureObservation,
    #[error("calendar fetch is later than the schedule observation")]
    FutureCalendar,
    #[error("schedule response has expired")]
    Expired,
    #[error("schedule validity exceeds the response freshness limit")]
    ResponseLifetime,
    #[error("calendar evidence is too old")]
    StaleCalendar,
    #[error("calendar coverage has not started")]
    CoverageNotStarted,
    #[error("calendar coverage has ended")]
    CoverageEnded,
    #[error("schedule validity exceeds calendar coverage")]
    ValidityOutsideCoverage,
    #[error("schedule validity exceeds the calendar freshness limit")]
    CalendarLifetime,
    #[error("schedule validity does not follow its observation")]
    InvalidValidityOrder,
    #[error("calendar revision is empty")]
    EmptyCalendarRevision,
}

impl TradingScheduleStore {
    pub(crate) async fn load(
        config: TradingScheduleConfig,
        pool: SqlitePool,
    ) -> Result<Self, TradingScheduleError> {
        let emergency_buffer = i64::try_from(config.emergency_buffer_secs.get())
            .ok()
            .and_then(chrono::TimeDelta::try_seconds)
            .ok_or(TradingScheduleError::Evidence)?;
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT scope_id, state_json FROM trading_schedule_latches WHERE environment = ?",
        )
        .bind(config.environment.as_str())
        .fetch_all(&pool)
        .await?;
        let mut latches = BTreeMap::new();
        for (scope, json) in rows {
            let latch: ScheduleLatch = serde_json::from_str(&json)?;
            validate_persisted_scope(&config, &scope, &latch.accepted.scope)?;
            latches.insert(scope, latch);
        }
        let rows = sqlx::query_as::<_, (String, String, i64, i64, i64)>(
            "SELECT scope_id, scope_json, started_at, closes_at, reached_at FROM trading_schedule_broker_windows WHERE environment = ?",
        ).bind(config.environment.as_str()).fetch_all(&pool).await?;
        let mut broker_windows = BTreeMap::new();
        for (scope, scope_json, start, close, reached) in rows {
            validate_persisted_scope(&config, &scope, &serde_json::from_str(&scope_json)?)?;
            let started_at =
                DateTime::from_timestamp_millis(start).ok_or(TradingScheduleError::Evidence)?;
            let closes_at =
                DateTime::from_timestamp_millis(close).ok_or(TradingScheduleError::Evidence)?;
            if started_at >= closes_at {
                return Err(TradingScheduleError::Evidence);
            }
            broker_windows.insert(
                scope,
                BrokerBoundary {
                    window: CloseFlattenWindow {
                        started_at,
                        closes_at,
                    },
                    reached_at: reached,
                },
            );
        }
        Ok(Self {
            config,
            emergency_buffer,
            pool,
            latches: Arc::new(RwLock::new(latches)),
            broker_windows: Arc::new(RwLock::new(broker_windows)),
            update_lock: Arc::new(tokio::sync::Mutex::new(())),
            changed: Arc::new(tokio::sync::Notify::new()),
        })
    }

    pub(crate) async fn accept(
        &self,
        scope: &TradingScheduleScope,
        response: TradingState,
        now: DateTime<Utc>,
    ) -> Result<(), TradingScheduleError> {
        let _update = self.update_lock.lock().await;
        validate_response(&self.config, scope, &response, now.timestamp_millis())?;
        let interval = match &response.schedule {
            Schedule::Open { current, .. } | Schedule::Draining { current, .. } => current.clone(),
            Schedule::Closed {
                previous: Some(previous),
                ..
            } => previous.clone(),
            Schedule::Closed { previous: None, .. } | Schedule::Unknown { .. } => {
                return Err(TradingScheduleError::Unknown);
            }
        };
        let previous = self
            .latches
            .read()
            .map_err(|_| TradingScheduleError::Lock)?
            .get(&scope.id)
            .cloned();
        let latch = merge_latch(previous, response, interval, now.timestamp_millis())?;
        let obsolete_fallback = self
            .broker_windows
            .read()
            .map_err(|_| TradingScheduleError::Lock)?
            .get(&scope.id)
            .is_some_and(|boundary| {
                latch.interval.opens_at.get() >= boundary.window.closes_at.timestamp_millis()
                    && now.timestamp_millis() >= latch.interval.opens_at.get()
            });
        if obsolete_fallback {
            sqlx::query("DELETE FROM trading_schedule_broker_windows WHERE environment = ? AND scope_id = ?")
                .bind(self.config.environment.as_str()).bind(&scope.id).execute(&self.pool).await?;
            self.broker_windows
                .write()
                .map_err(|_| TradingScheduleError::Lock)?
                .remove(&scope.id);
        }
        info!(scope = %scope.id, interval = %latch.interval.id.0,
            revision = %latch.accepted.policy_revision.0, cutoff_unix_ms = latch.interval.execution_cutoff.get(),
            close_unix_ms = latch.interval.hedge_close.get(), observed_at_unix_ms = latch.accepted.observed_at.get(),
            valid_until_unix_ms = latch.accepted.valid_until.get(), conflicted = latch.conflicted,
            schedule_age_ms = now.timestamp_millis().saturating_sub(latch.accepted.observed_at.get()),
            calendar_age_ms = ?latch.accepted.calendar.as_ref().map(|calendar| now.timestamp_millis().saturating_sub(calendar.fetched_at.get())),
            remaining_ms = latch.interval.hedge_close.get().saturating_sub(now.timestamp_millis()),
            "Accepted conservative trading schedule");
        metrics::counter!("trading_schedule_accepted_total", "scope" => scope.id.clone())
            .increment(1);
        if latch.conflicted {
            warn!(scope = %scope.id, "Trading schedule revisions conflict; retaining restrictive boundaries");
        }
        let json = serde_json::to_string(&latch)?;
        sqlx::query("INSERT INTO trading_schedule_latches (environment, scope_id, state_json) VALUES (?, ?, ?) ON CONFLICT(environment, scope_id) DO UPDATE SET state_json = excluded.state_json")
            .bind(self.config.environment.as_str()).bind(&scope.id).bind(json).execute(&self.pool).await?;
        self.latches
            .write()
            .map_err(|_| TradingScheduleError::Lock)?
            .insert(scope.id.clone(), latch);
        self.changed.notify_one();
        Ok(())
    }

    fn scope_for(&self, symbol: &Symbol) -> Option<&TradingScheduleScope> {
        self.config
            .scopes
            .iter()
            .find(|scope| scope.assets.iter().any(|asset| asset == symbol.as_str()))
    }

    pub(crate) fn window(
        &self,
        symbol: &Symbol,
        status: MarketSessionStatus,
        now: DateTime<Utc>,
    ) -> Option<CloseFlattenWindow> {
        let scope = self.scope_for(symbol)?;
        let broker_close = if scope.extended_hours {
            status.extended_session_closes_at
        } else {
            status.regular_session_closes_at
        };
        let schedules = match self.latches.read() {
            Ok(schedules) => schedules,
            Err(error) => {
                error!(%error, "Schedule latch unavailable; cannot assert closure protection");
                return None;
            }
        };
        let latched = schedules.get(&scope.id).and_then(|latch| {
            debug!(scope = %scope.id, mode = ?self.config.mode, interval = %latch.interval.id.0,
                remaining_ms = latch.interval.hedge_close.get().saturating_sub(now.timestamp_millis()),
                "Evaluating latched close-flatten window");
            let opens_at = DateTime::from_timestamp_millis(latch.interval.opens_at.get())?;
            let started_at =
                DateTime::from_timestamp_millis(latch.interval.execution_cutoff.get())?;
            let closes_at = DateTime::from_timestamp_millis(latch.interval.hedge_close.get())?;
            let effective_now = now.timestamp_millis().max(latch.reached_boundary);
            let rollback = chrono::TimeDelta::milliseconds(effective_now - now.timestamp_millis());
            (effective_now >= opens_at.timestamp_millis()
                && effective_now < closes_at.timestamp_millis())
            .then_some(CloseFlattenWindow {
                started_at: started_at.checked_sub_signed(rollback).unwrap_or(DateTime::<Utc>::MIN_UTC),
                closes_at: closes_at.checked_sub_signed(rollback).unwrap_or(DateTime::<Utc>::MIN_UTC),
            })
        });
        drop(schedules);
        let fallback = self
            .broker_windows
            .read()
            .inspect_err(|error| error!(%error, "Broker boundary unavailable; cannot assert closure protection"))
            .ok()?
            .get(&scope.id)
            .copied()
            .map(|boundary| {
                let rollback = chrono::TimeDelta::milliseconds(
                    boundary.reached_at.saturating_sub(now.timestamp_millis()).max(0),
                );
                CloseFlattenWindow {
                    started_at: boundary.window.started_at.checked_sub_signed(rollback).unwrap_or(DateTime::<Utc>::MIN_UTC),
                    closes_at: boundary.window.closes_at.checked_sub_signed(rollback).unwrap_or(DateTime::<Utc>::MIN_UTC),
                }
            });
        let latched = match (latched, fallback.filter(|window| now < window.closes_at)) {
            (Some(pricing), Some(broker)) => Some(CloseFlattenWindow {
                started_at: pricing.started_at.min(broker.started_at),
                closes_at: pricing.closes_at.min(broker.closes_at),
            }),
            (Some(window), None) | (None, Some(window)) => Some(window),
            (None, None) => None,
        };
        let proposed = match (latched, broker_close) {
            (Some(window), Some(broker_close)) if broker_close < window.closes_at => {
                warn!(%symbol, %broker_close, scheduled_close = %window.closes_at,
                    "Broker closes earlier than pricing schedule; using restrictive fallback");
                Some(CloseFlattenWindow {
                    started_at: window.started_at.min(broker_close - self.emergency_buffer),
                    closes_at: broker_close,
                })
            }
            (Some(window), _) => Some(window),
            (None, Some(closes_at)) => {
                metrics::counter!("trading_schedule_fallback_total", "scope" => scope.id.clone(), "reason" => "broker_close").increment(1);
                Some(CloseFlattenWindow {
                    started_at: closes_at - self.emergency_buffer,
                    closes_at,
                })
            }
            (None, None) => {
                metrics::counter!("trading_schedule_fallback_total", "scope" => scope.id.clone(), "reason" => "unknown_close").increment(1);
                warn!(%symbol, "Neither pricing schedule nor broker close is available");
                None
            }
        };
        proposed.filter(|window| now >= window.started_at && now < window.closes_at)
    }

    pub(crate) fn enabled(&self) -> bool {
        match self.config.mode {
            TradingScheduleMode::Observe => false,
            TradingScheduleMode::Enabled => true,
        }
    }

    pub(crate) async fn observe_broker(
        &self,
        symbol: &Symbol,
        status: MarketSessionStatus,
    ) -> Result<(), TradingScheduleError> {
        let Some(scope) = self.scope_for(symbol) else {
            return Ok(());
        };
        let Some(close) = (if scope.extended_hours {
            status.extended_session_closes_at
        } else {
            status.regular_session_closes_at
        }) else {
            debug!(%symbol, scope = %scope.id, session = ?status.session,
                "Broker close metadata unavailable; retaining existing schedule protection");
            return Ok(());
        };
        let _update = self.update_lock.lock().await;
        let fallback = self
            .broker_windows
            .read()
            .map_err(|_| TradingScheduleError::Lock)?
            .get(&scope.id)
            .copied();
        let previous = self
            .latches
            .read()
            .map_err(|_| TradingScheduleError::Lock)?
            .get(&scope.id)
            .cloned();
        let previous_fallback = fallback.filter(|boundary| {
            !status
                .session_opens_at
                .is_some_and(|opens| opens >= boundary.window.closes_at && opens <= Utc::now())
        });
        let window = CloseFlattenWindow {
            started_at: previous_fallback.map_or_else(
                || close - self.emergency_buffer,
                |boundary| {
                    boundary
                        .window
                        .started_at
                        .min(close - self.emergency_buffer)
                },
            ),
            closes_at: previous_fallback
                .map_or(close, |boundary| boundary.window.closes_at.min(close)),
        };
        if fallback.map(|boundary| boundary.window) != Some(window)
            && (fallback.is_some() || previous.as_ref().is_none_or(|latch| latch.broker_reopened))
        {
            let reached = previous_fallback.map_or(0, |boundary| boundary.reached_at);
            let identity = TradingScope {
                id: st0x_pricing_types::trading_state::ScopeId(scope.id.clone()),
                profile_revision: st0x_pricing_types::trading_state::Revision(
                    scope.profile_revision.clone(),
                ),
                sessions: if scope.extended_hours {
                    EligibleSessions::Extended
                } else {
                    EligibleSessions::Regular
                },
                assets: scope.assets.clone(),
            };
            sqlx::query("INSERT INTO trading_schedule_broker_windows (environment, scope_id, scope_json, started_at, closes_at, reached_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(environment, scope_id) DO UPDATE SET started_at = excluded.started_at, closes_at = excluded.closes_at, reached_at = excluded.reached_at")
                .bind(self.config.environment.as_str()).bind(&scope.id).bind(serde_json::to_string(&identity)?).bind(window.started_at.timestamp_millis()).bind(window.closes_at.timestamp_millis()).bind(reached).execute(&self.pool).await?;
            self.broker_windows
                .write()
                .map_err(|_| TradingScheduleError::Lock)?
                .insert(
                    scope.id.clone(),
                    BrokerBoundary {
                        window,
                        reached_at: reached,
                    },
                );
            self.changed.notify_one();
        }
        let Some(mut latch) = previous else {
            return Ok(());
        };
        if status.session != st0x_execution::MarketSession::Closed
            && status.session_opens_at.is_some_and(|opens| {
                opens.timestamp_millis() >= latch.interval.hedge_close.get() && opens <= Utc::now()
            })
        {
            latch.broker_reopened = true;
            sqlx::query("UPDATE trading_schedule_latches SET state_json = ? WHERE environment = ? AND scope_id = ?")
                .bind(serde_json::to_string(&latch)?).bind(self.config.environment.as_str()).bind(&scope.id).execute(&self.pool).await?;
            self.latches
                .write()
                .map_err(|_| TradingScheduleError::Lock)?
                .insert(scope.id.clone(), latch);
            return Ok(());
        }
        if close.timestamp_millis() >= latch.interval.hedge_close.get() {
            return Ok(());
        }
        let cutoff = close - self.emergency_buffer;
        if cutoff.timestamp_millis() <= latch.interval.opens_at.get() {
            return Err(TradingScheduleError::Evidence);
        }
        latch.interval.hedge_close = close
            .timestamp_millis()
            .try_into()
            .map_err(|_| TradingScheduleError::Evidence)?;
        latch.interval.execution_cutoff = latch.interval.execution_cutoff.min(
            cutoff
                .timestamp_millis()
                .try_into()
                .map_err(|_| TradingScheduleError::Evidence)?,
        );
        latch.conflicted = true;
        sqlx::query("UPDATE trading_schedule_latches SET state_json = ? WHERE environment = ? AND scope_id = ?")
            .bind(serde_json::to_string(&latch)?).bind(self.config.environment.as_str()).bind(&scope.id).execute(&self.pool).await?;
        self.latches
            .write()
            .map_err(|_| TradingScheduleError::Lock)?
            .insert(scope.id.clone(), latch);
        self.changed.notify_one();
        warn!(%symbol, %close, "Broker closed earlier than pricing; restrictive boundary persisted");
        Ok(())
    }

    pub(crate) fn allows_new_order(&self, symbol: &Symbol, now: DateTime<Utc>) -> bool {
        if !self.enabled() {
            return true;
        }
        let Some(scope) = self.scope_for(symbol) else {
            warn!(%symbol, "Missing trading schedule scope; broker checks remain authoritative");
            return true;
        };
        if !self
            .broker_windows
            .read()
            .inspect_err(
                |error| error!(%error, "Broker boundary unavailable; refusing order admission"),
            )
            .is_ok_and(|windows| {
                windows.get(&scope.id).is_none_or(|boundary| {
                    now.timestamp_millis().max(boundary.reached_at)
                        < boundary.window.closes_at.timestamp_millis()
                })
            })
        {
            return false;
        }
        let latches = match self.latches.read() {
            Ok(latches) => latches,
            Err(error) => {
                error!(%error, "Cannot read close safety latch");
                return false;
            }
        };
        let previous = latches.get(&scope.id).cloned();
        drop(latches);
        let Some(latch) = previous else {
            return true;
        };
        if latch.broker_reopened
            || now.timestamp_millis().max(latch.reached_boundary) < latch.interval.hedge_close.get()
        {
            return true;
        }
        let next = match &latch.accepted.schedule {
            Schedule::Open { next, .. }
            | Schedule::Draining { next, .. }
            | Schedule::Closed { next, .. } => next.as_ref(),
            Schedule::Unknown { .. } => None,
        };
        // Only ends the previous interval's restriction. Live broker checks still
        // decide whether an order can execute; cached data never grants eligibility.
        next.is_some_and(|next| now.timestamp_millis() >= next.opens_at.get())
    }

    pub(crate) fn next_boundary(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, TradingScheduleError> {
        let broker_boundaries: Vec<_> = self
            .broker_windows
            .read()
            .map_err(|_| TradingScheduleError::Lock)?
            .values()
            .flat_map(|boundary| {
                let effective_now = now.timestamp_millis().max(boundary.reached_at);
                [boundary.window.started_at, boundary.window.closes_at]
                    .into_iter()
                    .filter(move |boundary| boundary.timestamp_millis() > effective_now)
                    .map(move |boundary| {
                        boundary
                            - chrono::TimeDelta::milliseconds(
                                effective_now - now.timestamp_millis(),
                            )
                    })
            })
            .collect();
        Ok(self
            .latches
            .read()
            .map_err(|_| TradingScheduleError::Lock)?
            .values()
            .flat_map(|latch| {
                let effective_now = now.timestamp_millis().max(latch.reached_boundary);
                [latch.interval.execution_cutoff, latch.interval.hedge_close]
                    .into_iter()
                    .filter(move |boundary| boundary.get() > effective_now)
                    .filter_map(move |boundary| {
                        DateTime::from_timestamp_millis(
                            boundary.get() - (effective_now - now.timestamp_millis()),
                        )
                    })
            })
            .chain(broker_boundaries)
            .filter(|time| *time > now)
            .min())
    }

    async fn due_wakeups(&self, now: DateTime<Utc>) -> Result<Vec<String>, TradingScheduleError> {
        let _update = self.update_lock.lock().await;
        let windows = self
            .broker_windows
            .read()
            .map_err(|_| TradingScheduleError::Lock)?
            .clone();
        for (scope, mut boundary) in windows {
            let reached =
                (now >= boundary.window.started_at).then_some(now.min(boundary.window.closes_at));
            if let Some(reached) = reached
                && reached.timestamp_millis() > boundary.reached_at
            {
                sqlx::query("UPDATE trading_schedule_broker_windows SET reached_at = ? WHERE environment = ? AND scope_id = ?")
                        .bind(reached.timestamp_millis()).bind(self.config.environment.as_str()).bind(&scope).execute(&self.pool).await?;
                boundary.reached_at = reached.timestamp_millis();
                self.broker_windows
                    .write()
                    .map_err(|_| TradingScheduleError::Lock)?
                    .insert(scope, boundary);
            }
        }
        let mut latches = self
            .latches
            .read()
            .map_err(|_| TradingScheduleError::Lock)?
            .clone();
        for (scope, latch) in &mut latches {
            let reached = (now.timestamp_millis() >= latch.interval.execution_cutoff.get())
                .then_some(now.timestamp_millis().min(latch.interval.hedge_close.get()));
            if let Some(reached) = reached.filter(|reached| *reached > latch.reached_boundary) {
                latch.reached_boundary = reached;
                sqlx::query("UPDATE trading_schedule_latches SET state_json = ? WHERE environment = ? AND scope_id = ?")
                    .bind(serde_json::to_string(latch)?).bind(self.config.environment.as_str()).bind(scope).execute(&self.pool).await?;
            }
        }
        let broker_wakeups: Vec<_> = self
            .broker_windows
            .read()
            .map_err(|_| TradingScheduleError::Lock)?
            .iter()
            .flat_map(|(scope, boundary)| {
                let effective_now = now.timestamp_millis().max(boundary.reached_at);
                [boundary.window.started_at, boundary.window.closes_at]
                    .into_iter()
                    .filter(move |boundary| boundary.timestamp_millis() <= effective_now)
                    .map(move |boundary| {
                        format!(
                            "trading-schedule-broker:{scope}:{}",
                            boundary.timestamp_millis()
                        )
                    })
            })
            .collect();
        let wakeups = latches
            .iter()
            .flat_map(|(scope, latch)| {
                [latch.interval.execution_cutoff, latch.interval.hedge_close]
                    .into_iter()
                    .filter(move |boundary| {
                        boundary.get() <= now.timestamp_millis().max(latch.reached_boundary)
                    })
                    .map(move |boundary| {
                        format!(
                            "trading-schedule:{scope}:{}:{}",
                            latch.interval.id.0,
                            boundary.get()
                        )
                    })
            })
            .chain(broker_wakeups)
            .collect();
        *self
            .latches
            .write()
            .map_err(|_| TradingScheduleError::Lock)? = latches;
        Ok(wakeups)
    }
}

fn validate_persisted_scope(
    config: &TradingScheduleConfig,
    id: &str,
    persisted: &TradingScope,
) -> Result<(), TradingScheduleError> {
    let scope = config
        .scopes
        .iter()
        .find(|scope| scope.id == id)
        .ok_or(TradingScheduleError::Identity)?;
    let extended = match persisted.sessions {
        EligibleSessions::Regular => false,
        EligibleSessions::Extended => true,
    };
    let expected: BTreeSet<_> = scope.assets.iter().collect();
    let actual: BTreeSet<_> = persisted.assets.iter().collect();
    if persisted.id.0 != id
        || persisted.profile_revision.0 != scope.profile_revision
        || extended != scope.extended_hours
        || expected != actual
        || actual.len() != persisted.assets.len()
    {
        return Err(TradingScheduleError::Identity);
    }
    Ok(())
}

fn validate_response(
    config: &TradingScheduleConfig,
    scope: &TradingScheduleScope,
    response: &TradingState,
    now: i64,
) -> Result<(), TradingScheduleError> {
    let environment = match config.environment {
        TradingScheduleEnvironment::Staging => {
            st0x_pricing_types::trading_state::Environment::Staging
        }
        TradingScheduleEnvironment::Production => {
            st0x_pricing_types::trading_state::Environment::Production
        }
    };
    let sessions_match = match response.scope.sessions {
        EligibleSessions::Regular => !scope.extended_hours,
        EligibleSessions::Extended => scope.extended_hours,
    };
    let expected: BTreeSet<_> = scope.assets.iter().collect();
    let actual: BTreeSet<_> = response.scope.assets.iter().collect();
    if response.schema_version != SCHEMA_VERSION
        || environment != response.environment
        || response.scope.id.0 != scope.id
        || response.scope.profile_revision.0 != scope.profile_revision
        || !sessions_match
        || expected != actual
        || actual.len() != response.scope.assets.len()
        || response.policy_revision.0.trim().is_empty()
    {
        return Err(TradingScheduleError::Identity);
    }
    let Some(calendar) = &response.calendar else {
        return Err(TradingScheduleError::Unknown);
    };
    let millis = |seconds: u64| {
        i64::try_from(seconds)
            .ok()
            .and_then(|value| value.checked_mul(1000))
            .ok_or(TradingScheduleError::Evidence)
    };
    let skew = millis(config.evidence_clock_skew_secs.get())?;
    let freshness = millis(config.response_freshness_secs.get())?;
    let calendar_max_age = millis(config.calendar_max_age_secs.get())?;
    let checks = [
        (
            response.observed_at.get() > now.saturating_add(skew),
            ResponseEvidenceError::FutureObservation,
        ),
        (
            calendar.fetched_at.get() > response.observed_at.get().saturating_add(skew),
            ResponseEvidenceError::FutureCalendar,
        ),
        (
            now >= response.valid_until.get(),
            ResponseEvidenceError::Expired,
        ),
        (
            response.valid_until.get() > response.observed_at.get().saturating_add(freshness),
            ResponseEvidenceError::ResponseLifetime,
        ),
        (
            now.saturating_sub(calendar.fetched_at.get()) > calendar_max_age,
            ResponseEvidenceError::StaleCalendar,
        ),
        (
            calendar.coverage_start.get() > now,
            ResponseEvidenceError::CoverageNotStarted,
        ),
        (
            calendar.coverage_end.get() <= now,
            ResponseEvidenceError::CoverageEnded,
        ),
        (
            response.valid_until > calendar.coverage_end,
            ResponseEvidenceError::ValidityOutsideCoverage,
        ),
        (
            response.valid_until.get() > calendar.fetched_at.get().saturating_add(calendar_max_age),
            ResponseEvidenceError::CalendarLifetime,
        ),
        (
            response.valid_until <= response.observed_at,
            ResponseEvidenceError::InvalidValidityOrder,
        ),
        (
            calendar.revision.0.trim().is_empty(),
            ResponseEvidenceError::EmptyCalendarRevision,
        ),
    ];
    if let Some((_, reason)) = checks.into_iter().find(|(invalid, _)| *invalid) {
        return Err(reason.into());
    }
    validate_intervals(response, calendar)
}

fn validate_intervals(
    response: &TradingState,
    calendar: &CalendarEvidence,
) -> Result<(), TradingScheduleError> {
    let observed = response.observed_at.get();
    let (interval, next) = match &response.schedule {
        Schedule::Open { current, next }
            if current.opens_at.get() <= observed && observed < current.execution_cutoff.get() =>
        {
            (Some(current), next.as_ref())
        }
        Schedule::Draining { current, next }
            if current.execution_cutoff.get() <= observed
                && observed < current.hedge_close.get() =>
        {
            (Some(current), next.as_ref())
        }
        Schedule::Closed { previous, next }
            if previous
                .as_ref()
                .is_none_or(|interval| interval.hedge_close.get() <= observed)
                && next
                    .as_ref()
                    .is_none_or(|interval| observed < interval.opens_at.get()) =>
        {
            (previous.as_ref(), next.as_ref())
        }
        Schedule::Open { .. } | Schedule::Draining { .. } | Schedule::Closed { .. } => {
            return Err(TradingScheduleError::Evidence);
        }
        Schedule::Unknown { .. } => return Err(TradingScheduleError::Unknown),
    };
    if interval.zip(next).is_some_and(|(current, next)| {
        let overlaps = next.opens_at < current.hedge_close;
        overlaps || next.id == current.id
    }) {
        return Err(TradingScheduleError::Overlap);
    }
    for interval in interval.into_iter().chain(next) {
        let outside_coverage = interval.opens_at < calendar.coverage_start
            || interval.hedge_close >= calendar.coverage_end;
        if interval.id.0.trim().is_empty()
            || interval.opens_at >= interval.execution_cutoff
            || interval.execution_cutoff >= interval.hedge_close
            || outside_coverage
            || DateTime::<Utc>::from_timestamp_millis(interval.hedge_close.get()).is_none()
        {
            return Err(TradingScheduleError::Evidence);
        }
    }
    Ok(())
}

fn merge_latch(
    previous: Option<ScheduleLatch>,
    response: TradingState,
    mut interval: TradingInterval,
    now: i64,
) -> Result<ScheduleLatch, TradingScheduleError> {
    let Some(previous) = previous else {
        return Ok(ScheduleLatch {
            accepted: response,
            interval,
            conflicted: false,
            broker_reopened: false,
            reached_boundary: now,
        });
    };
    if previous.interval.id == interval.id && previous.interval.opens_at == interval.opens_at {
        let conflicted =
            previous.conflicted || previous.accepted.policy_revision != response.policy_revision;
        interval.execution_cutoff = interval
            .execution_cutoff
            .min(previous.interval.execution_cutoff);
        interval.hedge_close = interval.hedge_close.min(previous.interval.hedge_close);
        return Ok(ScheduleLatch {
            accepted: response,
            interval,
            conflicted,
            broker_reopened: previous.broker_reopened,
            reached_boundary: previous.reached_boundary.max(now),
        });
    }
    if now < previous.interval.hedge_close.get()
        || interval.opens_at < previous.interval.hedge_close
        || now < interval.opens_at.get()
    {
        return Err(TradingScheduleError::Overlap);
    }
    Ok(ScheduleLatch {
        accepted: response,
        interval,
        conflicted: false,
        broker_reopened: false,
        reached_boundary: now,
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use st0x_execution::{
        ClientOrderId, Direction, FractionalShares, MarketOrder, MarketSession, MockExecutor,
        Positive,
    };
    use st0x_float_macro::float;

    use crate::offchain::order::{
        CounterTradeOrderKind, ExecutorOrderPlacer, OrderPlacer, PlacementAdmission,
    };
    use crate::trading::offchain::close_flatten::CloseFlattenPolicy;

    use super::*;

    pub(super) fn response(cutoff: i64, close: i64) -> TradingState {
        serde_json::from_value(json!({
            "schema_version": 1, "environment": "staging",
            "scope": {"id": "regular", "profile_revision": "v1", "sessions": "regular", "assets": ["AAPL"]},
            "policy_revision": "v1", "observed_at": 1000, "valid_until": 31000,
            "calendar": {"fetched_at": 1000, "coverage_start": 1, "coverage_end": 100_000, "revision": "calendar"},
            "schedule": {"phase": "open", "current": {"id": "first", "opens_at": 1000, "execution_cutoff": cutoff, "hedge_close": close}, "next": null},
            "quote_availability": {"status": "available"}
        })).unwrap()
    }

    fn interval(response: &TradingState) -> TradingInterval {
        match &response.schedule {
            Schedule::Open { current, .. } => current.clone(),
            _ => panic!("expected open fixture"),
        }
    }

    pub(super) fn config() -> TradingScheduleConfig {
        toml::from_str(
            r#"
            mode = "enabled"
            environment = "staging"
            poll_interval_secs = 5
            request_timeout_secs = 3
            response_freshness_secs = 30
            calendar_max_age_secs = 7200
            evidence_clock_skew_secs = 2
            emergency_buffer_secs = 900
            [[scopes]]
            id = "regular"
            profile_revision = "v1"
            extended_hours = false
            assets = ["AAPL"]
        "#,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn load_rejects_unrepresentable_emergency_buffer() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        for seconds in [u64::MAX, i64::MAX.cast_unsigned()] {
            let mut config = config();
            config.emergency_buffer_secs = std::num::NonZeroU64::new(seconds).unwrap();
            assert!(matches!(
                TradingScheduleStore::load(config, pool.clone()).await,
                Err(TradingScheduleError::Evidence)
            ));
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn rollback_preserves_boundaries_and_persisted_progress(
            cutoff in prop_oneof![Just(1_000_000i64), Just(DateTime::<Utc>::MAX_UTC.timestamp_millis() - 900_001), 1_000_000i64..=DateTime::<Utc>::MAX_UTC.timestamp_millis() - 900_001],
            elapsed in prop_oneof![Just(0i64), Just(900_000i64), 0i64..=900_000],
            raw_now in prop_oneof![Just(DateTime::<Utc>::MIN_UTC.timestamp_millis()), Just(DateTime::<Utc>::MAX_UTC.timestamp_millis()), DateTime::<Utc>::MIN_UTC.timestamp_millis()..=DateTime::<Utc>::MAX_UTC.timestamp_millis()],
        ) {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let close = cutoff + 900_000;
                let progress = cutoff + elapsed;
                let now_ms = raw_now.min(progress);
                let now = DateTime::from_timestamp_millis(now_ms).unwrap();
                let symbol = Symbol::new("AAPL").unwrap();
                for pricing in [true, false] {
                    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
                    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
                    let config = config();
                    let store = TradingScheduleStore::load(config.clone(), pool.clone()).await.unwrap();
                    if pricing {
                        let mut response = response(cutoff, close);
                        response.calendar.as_mut().unwrap().coverage_end = (close + 1).try_into().unwrap();
                        store.accept(&config.scopes[0], response,
                            DateTime::from_timestamp_millis(1000).unwrap()).await.unwrap();
                    } else {
                        let mut status = MarketSessionStatus::without_close_metadata(MarketSession::Regular);
                        status.regular_session_closes_at = DateTime::from_timestamp_millis(close);
                        store.observe_broker(&symbol, status).await.unwrap();
                    }
                    let due = store.due_wakeups(DateTime::from_timestamp_millis(progress).unwrap()).await.unwrap();
                    drop(store);
                    let store = TradingScheduleStore::load(config.clone(), pool.clone()).await.unwrap();
                    prop_assert_eq!(store.due_wakeups(now).await.unwrap(), due);
                    let restored = TradingScheduleStore::load(config, pool).await.unwrap();
                    let reached = if pricing {
                        restored.latches.read().unwrap()["regular"].reached_boundary
                    } else {
                        restored.broker_windows.read().unwrap()["regular"].reached_at
                    };
                    prop_assert_eq!(reached, progress);
                    let next = restored.next_boundary(now).unwrap();
                    let window = restored.window(&symbol,
                        MarketSessionStatus::without_close_metadata(MarketSession::Regular), now);
                    if elapsed == 900_000 {
                        prop_assert_eq!(next, None);
                        prop_assert_eq!(window, None);
                    } else {
                        let expected_close = DateTime::from_timestamp_millis(now_ms + 900_000 - elapsed).unwrap();
                        prop_assert_eq!(next, Some(expected_close));
                        let window = window.expect("rollback must retain the active flatten window");
                        prop_assert_eq!(window.closes_at, expected_close);
                        prop_assert_eq!(window.started_at.timestamp_millis(),
                            (now_ms - elapsed).max(DateTime::<Utc>::MIN_UTC.timestamp_millis()));
                        prop_assert!(window.started_at.timestamp_millis() <= cutoff);
                        prop_assert!(window.closes_at.timestamp_millis() <= close);
                    }
                }
                Ok(())
            })?;
        }
    }

    #[tokio::test]
    async fn restart_rejects_scope_changes_for_pricing_and_broker_latches() {
        for pricing in [true, false] {
            let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
            sqlx::migrate!("./migrations").run(&pool).await.unwrap();
            let original = config();
            let store = TradingScheduleStore::load(original.clone(), pool.clone())
                .await
                .unwrap();
            if pricing {
                store
                    .accept(
                        &original.scopes[0],
                        response(2000, 4000),
                        DateTime::from_timestamp_millis(1000).unwrap(),
                    )
                    .await
                    .unwrap();
            } else {
                let mut status =
                    MarketSessionStatus::without_close_metadata(MarketSession::Regular);
                status.regular_session_closes_at = DateTime::from_timestamp_millis(4_000_000);
                store
                    .observe_broker(&Symbol::new("AAPL").unwrap(), status)
                    .await
                    .unwrap();
            }
            drop(store);
            let mut renamed = original.clone();
            renamed.scopes[0].id = "replacement".into();
            let mut moved = original.clone();
            moved.scopes[0].assets = vec!["MSFT".into()];
            let mut extended = original.clone();
            extended.scopes[0].extended_hours = true;
            let mut revised = original.clone();
            revised.scopes[0].profile_revision = "v2".into();
            for changed in [renamed, moved, extended, revised] {
                assert!(matches!(
                    TradingScheduleStore::load(changed, pool.clone())
                        .await
                        .unwrap_err(),
                    TradingScheduleError::Identity
                ));
            }
            TradingScheduleStore::load(original, pool).await.unwrap();
        }
    }

    #[tokio::test]
    async fn rollback_preserves_due_wakeups_and_effective_next_boundary() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let config = config();
        let store = TradingScheduleStore::load(config.clone(), pool.clone())
            .await
            .unwrap();
        store
            .accept(
                &config.scopes[0],
                response(2000, 4000),
                DateTime::from_timestamp_millis(1000).unwrap(),
            )
            .await
            .unwrap();
        store
            .due_wakeups(DateTime::from_timestamp_millis(2500).unwrap())
            .await
            .unwrap();
        drop(store);
        let store = TradingScheduleStore::load(config, pool).await.unwrap();
        let rollback = DateTime::from_timestamp_millis(1500).unwrap();
        assert_eq!(
            store.due_wakeups(rollback).await.unwrap(),
            ["trading-schedule:regular:first:2000"]
        );
        assert_eq!(
            store.next_boundary(rollback).unwrap(),
            DateTime::from_timestamp_millis(3000)
        );
    }

    #[tokio::test]
    async fn broker_rollback_preserves_due_wakeups_and_effective_next_boundary() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let config = config();
        let store = TradingScheduleStore::load(config.clone(), pool.clone())
            .await
            .unwrap();
        let mut status = MarketSessionStatus::without_close_metadata(MarketSession::Regular);
        status.regular_session_closes_at = DateTime::from_timestamp_millis(4_000_000);
        store
            .observe_broker(&Symbol::new("AAPL").unwrap(), status)
            .await
            .unwrap();
        store
            .due_wakeups(DateTime::from_timestamp_millis(3_500_000).unwrap())
            .await
            .unwrap();
        drop(store);
        let store = TradingScheduleStore::load(config, pool).await.unwrap();
        let rollback = DateTime::from_timestamp_millis(3_000_000).unwrap();
        assert_eq!(
            store.due_wakeups(rollback).await.unwrap(),
            ["trading-schedule-broker:regular:3100000"]
        );
        assert_eq!(
            store.next_boundary(rollback).unwrap(),
            DateTime::from_timestamp_millis(3_500_000)
        );
    }

    #[tokio::test]
    async fn failed_progress_persistence_does_not_publish_or_lose_the_retry() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let config = config();
        let store = TradingScheduleStore::load(config.clone(), pool.clone())
            .await
            .unwrap();
        store
            .accept(
                &config.scopes[0],
                response(2000, 4000),
                DateTime::from_timestamp_millis(1000).unwrap(),
            )
            .await
            .unwrap();
        sqlx::query("ALTER TABLE trading_schedule_latches RENAME TO unavailable_latches")
            .execute(&pool)
            .await
            .unwrap();
        let cutoff = DateTime::from_timestamp_millis(2000).unwrap();
        assert!(matches!(
            store.due_wakeups(cutoff).await.unwrap_err(),
            TradingScheduleError::Database(_)
        ));
        assert_eq!(
            store.latches.read().unwrap()["regular"].reached_boundary,
            1000
        );
        sqlx::query("ALTER TABLE unavailable_latches RENAME TO trading_schedule_latches")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            store.due_wakeups(cutoff).await.unwrap(),
            ["trading-schedule:regular:first:2000"]
        );
        let restored = TradingScheduleStore::load(config, pool).await.unwrap();
        assert_eq!(
            restored.latches.read().unwrap()["regular"].reached_boundary,
            2000
        );
    }

    #[tokio::test]
    async fn final_admission_refuses_new_orders_after_broker_close() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let store = Arc::new(
            TradingScheduleStore::load(config(), pool.clone())
                .await
                .unwrap(),
        );
        let policy = CloseFlattenPolicy::from_secs(900)
            .unwrap()
            .with_schedule(Some(store));
        let close = Utc::now();
        let placer = ExecutorOrderPlacer {
            executor: MockExecutor::new()
                .with_market_session(MarketSession::Closed)
                .with_regular_session_closes_at(close),
            close_flatten_policy: Some(policy),
        };
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
        };
        assert!(matches!(
            placer
                .prepare_placement(&order, &CounterTradeOrderKind::Market)
                .await
                .unwrap(),
            PlacementAdmission::Deferred
        ));
        let restored = TradingScheduleStore::load(config(), pool).await.unwrap();
        assert!(!restored.allows_new_order(&order.symbol, close));
    }

    #[tokio::test]
    async fn regular_admission_persists_broker_close_before_future_metadata() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let store = Arc::new(
            TradingScheduleStore::load(config(), pool.clone())
                .await
                .unwrap(),
        );
        let policy = CloseFlattenPolicy::from_secs(900)
            .unwrap()
            .with_schedule(Some(store));
        let close = Utc::now() + chrono::TimeDelta::seconds(300);
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
        };
        for observed_close in [close, close + chrono::TimeDelta::seconds(900)] {
            let placer = ExecutorOrderPlacer {
                executor: MockExecutor::new().with_regular_session_closes_at(observed_close),
                close_flatten_policy: Some(policy.clone()),
            };
            assert!(matches!(
                placer
                    .prepare_placement(&order, &CounterTradeOrderKind::Market)
                    .await
                    .unwrap(),
                PlacementAdmission::New
            ));
        }
        let restored = TradingScheduleStore::load(config(), pool).await.unwrap();
        assert!(!restored.allows_new_order(&order.symbol, close));
        assert_eq!(
            restored.broker_windows.read().unwrap()["regular"]
                .window
                .closes_at
                .timestamp_millis(),
            close.timestamp_millis()
        );
    }

    #[tokio::test]
    async fn broker_window_schema_rejects_zero_and_negative_duration() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        for close in [999, 1000] {
            let error = sqlx::query("INSERT INTO trading_schedule_broker_windows VALUES ('staging', 'regular', '{}', 1000, ?, 0)")
                .bind(close).execute(&pool).await.unwrap_err();
            let sqlx::Error::Database(error) = error else {
                panic!("expected constraint failure: {error:?}");
            };
            assert_eq!(error.kind(), sqlx::error::ErrorKind::CheckViolation);
        }
    }

    #[test]
    fn durable_latch_json_and_optional_progress_defaults_are_stable() {
        let expected = json!({
            "accepted": {
                "schema_version": 1, "environment": "staging",
                "scope": {"id": "regular", "profile_revision": "v1", "sessions": "regular", "assets": ["AAPL"]},
                "policy_revision": "v1", "observed_at": 1000, "valid_until": 31000,
                "calendar": {"fetched_at": 1000, "coverage_start": 1, "coverage_end": 100_000, "revision": "calendar"},
                "schedule": {"phase": "open", "current": {"id": "first", "opens_at": 1000, "execution_cutoff": 2000, "hedge_close": 4000}, "next": null},
                "quote_availability": {"status": "available"}
            },
            "interval": {"id": "first", "opens_at": 1000, "execution_cutoff": 2000, "hedge_close": 4000},
            "conflicted": false, "broker_reopened": false, "reached_boundary": 1000
        });
        let accepted = response(2000, 4000);
        let latch = merge_latch(None, accepted.clone(), interval(&accepted), 1000).unwrap();
        assert_eq!(serde_json::to_value(&latch).unwrap(), expected);
        let mut without_progress = expected;
        without_progress
            .as_object_mut()
            .unwrap()
            .remove("broker_reopened");
        without_progress
            .as_object_mut()
            .unwrap()
            .remove("reached_boundary");
        let restored: ScheduleLatch = serde_json::from_value(without_progress).unwrap();
        assert!(!restored.broker_reopened);
        assert_eq!(restored.reached_boundary, 0);
        assert_eq!(
            serde_json::to_value(restored.accepted).unwrap(),
            serde_json::to_value(accepted).unwrap()
        );
    }

    #[tokio::test]
    async fn deferred_placement_retains_pending_intent_for_recovery() {
        assert_deferred_pending_recovery(MarketSession::Closed).await;
    }

    #[tokio::test]
    async fn mismatched_recovery_retains_pending_intent() {
        for (symbol, direction) in [("MSFT", Direction::Buy), ("AAPL", Direction::Sell)] {
            let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
            sqlx::migrate!("./migrations").run(&pool).await.unwrap();
            let schedule = Arc::new(
                TradingScheduleStore::load(config(), pool.clone())
                    .await
                    .unwrap(),
            );
            let policy = CloseFlattenPolicy::from_secs(900)
                .unwrap()
                .with_schedule(Some(schedule));
            let placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer {
                executor: MockExecutor::new().with_recovered_order(
                    st0x_execution::OrderPlacement {
                        order_id: "different-order".into(),
                        symbol: Symbol::new(symbol).unwrap(),
                        shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                        direction,
                        placed_at: Utc::now(),
                        extended_hours: false,
                        limit_price: None,
                    },
                ),
                close_flatten_policy: Some(policy),
            });
            let (store, _) = st0x_event_sorcery::StoreBuilder::<
                crate::offchain::order::OffchainOrder,
            >::new(pool)
            .build(placer.clone())
            .await
            .unwrap();
            let id = crate::offchain::order::OffchainOrderId::new();
            let placement = crate::offchain::order::OffchainOrderPlacement::market(
                Symbol::new("AAPL").unwrap(),
                Positive::new(FractionalShares::new(float!(1))).unwrap(),
                Direction::Buy,
                st0x_execution::SupportedExecutor::DryRun,
                ClientOrderId::from_uuid(id.as_uuid()),
            );
            for _ in 0..2 {
                let error = crate::offchain::order::place_offchain_order_at_broker(
                    &store,
                    placer.as_ref(),
                    &id,
                    placement.clone(),
                )
                .await
                .unwrap_err();
                assert!(matches!(
                    error,
                    crate::offchain::order::PlaceOffchainOrderError::Admission { .. }
                ));
                assert!(matches!(
                    store.load(&id).await.unwrap(),
                    Some(crate::offchain::order::OffchainOrder::Pending { .. })
                ));
            }
        }
    }

    #[tokio::test]
    async fn unpriced_pending_recovery_waits_for_regular_session_without_losing_intent() {
        assert_deferred_pending_recovery(MarketSession::Extended).await;
    }

    async fn assert_deferred_pending_recovery(session: MarketSession) {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let schedule = Arc::new(
            TradingScheduleStore::load(config(), pool.clone())
                .await
                .unwrap(),
        );
        let policy = CloseFlattenPolicy::from_secs(900)
            .unwrap()
            .with_schedule(Some(schedule));
        let placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer {
            executor: MockExecutor::new().with_market_session(session),
            close_flatten_policy: Some(policy),
        });
        let (store, _) =
            st0x_event_sorcery::StoreBuilder::<crate::offchain::order::OffchainOrder>::new(pool)
                .build(placer.clone())
                .await
                .unwrap();
        let id = crate::offchain::order::OffchainOrderId::new();
        let placement = crate::offchain::order::OffchainOrderPlacement::market(
            Symbol::new("AAPL").unwrap(),
            Positive::new(FractionalShares::new(float!(1))).unwrap(),
            Direction::Buy,
            st0x_execution::SupportedExecutor::DryRun,
            ClientOrderId::from_uuid(id.as_uuid()),
        );
        for _ in 0..2 {
            let error = crate::offchain::order::place_offchain_order_at_broker(
                &store,
                placer.as_ref(),
                &id,
                placement.clone(),
            )
            .await
            .unwrap_err();
            assert!(matches!(
                error,
                crate::offchain::order::PlaceOffchainOrderError::Deferred
            ));
            assert!(matches!(
                store.load(&id).await.unwrap(),
                Some(crate::offchain::order::OffchainOrder::Pending { .. })
            ));
        }
    }

    #[tokio::test]
    async fn final_admission_adopts_existing_order_even_after_close() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let store = Arc::new(TradingScheduleStore::load(config(), pool).await.unwrap());
        let policy = CloseFlattenPolicy::from_secs(900)
            .unwrap()
            .with_schedule(Some(store));
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let existing = st0x_execution::OrderPlacement {
            order_id: "existing".into(),
            symbol: symbol.clone(),
            shares,
            direction: Direction::Buy,
            placed_at: Utc::now(),
            extended_hours: false,
            limit_price: None,
        };
        let placer = ExecutorOrderPlacer {
            executor: MockExecutor::new()
                .with_market_session(MarketSession::Closed)
                .with_recovered_order(existing),
            close_flatten_policy: Some(policy),
        };
        let order = MarketOrder {
            symbol,
            shares,
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
        };
        let PlacementAdmission::Recovered(recovered) = placer
            .prepare_placement(&order, &CounterTradeOrderKind::Market)
            .await
            .unwrap()
        else {
            panic!("expected recovered order");
        };
        assert_eq!(recovered.executor_order_id.as_ref(), "existing");
        assert_eq!(recovered.placed_shares, shares);
    }

    #[tokio::test]
    async fn broker_earlier_close_survives_restart() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let config = config();
        let store = TradingScheduleStore::load(config.clone(), pool.clone())
            .await
            .unwrap();
        let mut response = response(2_000_000, 4_000_000);
        response.calendar.as_mut().unwrap().coverage_end = 5_000_000.try_into().unwrap();
        store
            .accept(
                &config.scopes[0],
                response,
                DateTime::from_timestamp_millis(1000).unwrap(),
            )
            .await
            .unwrap();
        let mut status = MarketSessionStatus::without_close_metadata(MarketSession::Regular);
        status.regular_session_closes_at = DateTime::from_timestamp_millis(3_000_000);
        store
            .observe_broker(&Symbol::new("AAPL").unwrap(), status)
            .await
            .unwrap();
        drop(store);
        let store = TradingScheduleStore::load(config, pool).await.unwrap();
        let window = store
            .window(
                &Symbol::new("AAPL").unwrap(),
                MarketSessionStatus::without_close_metadata(MarketSession::Regular),
                DateTime::from_timestamp_millis(2_500_000).unwrap(),
            )
            .unwrap();
        assert_eq!(window.closes_at.timestamp_millis(), 3_000_000);
        assert!(!store.allows_new_order(
            &Symbol::new("AAPL").unwrap(),
            DateTime::from_timestamp_millis(3_000_000).unwrap()
        ));
    }

    #[tokio::test]
    async fn broker_only_fallback_survives_restart_without_extending_its_close() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let config = config();
        let store = TradingScheduleStore::load(config.clone(), pool.clone())
            .await
            .unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        let mut status = MarketSessionStatus::without_close_metadata(MarketSession::Regular);
        status.session_opens_at = DateTime::from_timestamp_millis(1000);
        status.regular_session_closes_at = DateTime::from_timestamp_millis(4_000_000);
        store.observe_broker(&symbol, status).await.unwrap();
        drop(store);
        let store = TradingScheduleStore::load(config.clone(), pool.clone())
            .await
            .unwrap();
        status.regular_session_closes_at = DateTime::from_timestamp_millis(5_000_000);
        store.observe_broker(&symbol, status).await.unwrap();
        let now = DateTime::from_timestamp_millis(3_500_000).unwrap();
        let window = store.window(&symbol, status, now).unwrap();
        assert_eq!(window.closes_at.timestamp_millis(), 4_000_000);
        assert_eq!(window.started_at.timestamp_millis(), 3_100_000);
        assert!(
            !store.allows_new_order(&symbol, DateTime::from_timestamp_millis(4_000_000).unwrap())
        );
        assert_eq!(
            store.due_wakeups(now).await.unwrap(),
            vec!["trading-schedule-broker:regular:3100000".to_owned()]
        );
        drop(store);
        let store = TradingScheduleStore::load(config.clone(), pool.clone())
            .await
            .unwrap();
        assert_eq!(
            store
                .window(
                    &symbol,
                    status,
                    DateTime::from_timestamp_millis(3_000_000).unwrap()
                )
                .unwrap()
                .started_at
                .timestamp_millis(),
            2_600_000
        );
        store
            .due_wakeups(DateTime::from_timestamp_millis(4_000_000).unwrap())
            .await
            .unwrap();
        drop(store);
        let store = TradingScheduleStore::load(config, pool).await.unwrap();
        assert!(!store.allows_new_order(&symbol, now));
    }

    #[tokio::test]
    async fn restart_restores_earliest_deadline_despite_a_later_response() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let config = config();
        let store = TradingScheduleStore::load(config.clone(), pool.clone())
            .await
            .unwrap();
        store
            .accept(
                &config.scopes[0],
                response(2000, 4000),
                DateTime::from_timestamp_millis(1000).unwrap(),
            )
            .await
            .unwrap();
        drop(store);
        let store = TradingScheduleStore::load(config.clone(), pool)
            .await
            .unwrap();
        store
            .accept(
                &config.scopes[0],
                response(3000, 5000),
                DateTime::from_timestamp_millis(1100).unwrap(),
            )
            .await
            .unwrap();
        let window = store
            .window(
                &Symbol::new("AAPL").unwrap(),
                MarketSessionStatus::without_close_metadata(st0x_execution::MarketSession::Regular),
                DateTime::from_timestamp_millis(2000).unwrap(),
            )
            .unwrap();
        assert_eq!(window.started_at.timestamp_millis(), 2000);
        assert_eq!(window.closes_at.timestamp_millis(), 4000);
        assert_eq!(
            store
                .due_wakeups(DateTime::from_timestamp_millis(2000).unwrap())
                .await
                .unwrap(),
            ["trading-schedule:regular:first:2000"]
        );
    }

    #[test]
    fn successful_heartbeat_cannot_refresh_old_calendar_evidence() {
        let config = config();
        let mut response = response(8_001_000, 8_002_000);
        response.observed_at = 8_000_000.try_into().unwrap();
        response.valid_until = 8_030_000.try_into().unwrap();
        let calendar = response.calendar.as_mut().unwrap();
        calendar.fetched_at = 1.try_into().unwrap();
        calendar.coverage_end = 10_000_000.try_into().unwrap();
        assert!(matches!(
            validate_response(&config, &config.scopes[0], &response, 8_000_000).unwrap_err(),
            TradingScheduleError::ResponseEvidence(ResponseEvidenceError::StaleCalendar)
        ));
    }

    #[test]
    fn scope_mismatch_and_duplicate_membership_are_rejected() {
        let config = config();
        let mut response = response(2000, 4000);
        response.scope.assets.push("AAPL".into());
        assert!(matches!(
            validate_response(&config, &config.scopes[0], &response, 1000).unwrap_err(),
            TradingScheduleError::Identity
        ));
    }

    #[test]
    fn exact_response_expiry_is_invalid() {
        let config = config();
        assert!(matches!(
            validate_response(&config, &config.scopes[0], &response(2000, 4000), 31000)
                .unwrap_err(),
            TradingScheduleError::ResponseEvidence(ResponseEvidenceError::Expired)
        ));
    }

    #[test]
    fn rejected_response_evidence_identifies_the_failed_check() {
        for expected in [
            ResponseEvidenceError::FutureObservation,
            ResponseEvidenceError::FutureCalendar,
            ResponseEvidenceError::Expired,
            ResponseEvidenceError::ResponseLifetime,
            ResponseEvidenceError::StaleCalendar,
            ResponseEvidenceError::CoverageNotStarted,
            ResponseEvidenceError::CoverageEnded,
            ResponseEvidenceError::ValidityOutsideCoverage,
            ResponseEvidenceError::CalendarLifetime,
            ResponseEvidenceError::InvalidValidityOrder,
            ResponseEvidenceError::EmptyCalendarRevision,
        ] {
            let mut config = config();
            let mut response = response(2000, 4000);
            let mut now = 1000;
            let calendar = response.calendar.as_mut().unwrap();
            match expected {
                ResponseEvidenceError::FutureObservation => {
                    response.observed_at = 4000.try_into().unwrap();
                }
                ResponseEvidenceError::FutureCalendar => {
                    calendar.fetched_at = 4000.try_into().unwrap();
                }
                ResponseEvidenceError::Expired => now = 31000,
                ResponseEvidenceError::ResponseLifetime => {
                    response.valid_until = 32000.try_into().unwrap();
                }
                ResponseEvidenceError::StaleCalendar => {
                    now = 8_000_000;
                    response.observed_at = now.try_into().unwrap();
                    response.valid_until = (now + 30000).try_into().unwrap();
                    calendar.coverage_end = 10_000_000.try_into().unwrap();
                }
                ResponseEvidenceError::CoverageNotStarted => {
                    calendar.coverage_start = 2000.try_into().unwrap();
                }
                ResponseEvidenceError::CoverageEnded => {
                    calendar.coverage_end = 1000.try_into().unwrap();
                }
                ResponseEvidenceError::ValidityOutsideCoverage => {
                    calendar.coverage_end = 30000.try_into().unwrap();
                }
                ResponseEvidenceError::CalendarLifetime => {
                    config.calendar_max_age_secs = 10.try_into().unwrap();
                }
                ResponseEvidenceError::InvalidValidityOrder => {
                    now = 500;
                    response.valid_until = response.observed_at;
                }
                ResponseEvidenceError::EmptyCalendarRevision => calendar.revision.0.clear(),
            }
            let error = validate_response(&config, &config.scopes[0], &response, now).unwrap_err();
            let TradingScheduleError::ResponseEvidence(actual) = error else {
                panic!("expected {expected:?}, got {error:?}");
            };
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn observation_keeps_legacy_window_while_enabled_uses_pricing_window() {
        for mode in [TradingScheduleMode::Observe, TradingScheduleMode::Enabled] {
            let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
            sqlx::migrate!("./migrations").run(&pool).await.unwrap();
            let mut config = config();
            config.mode = mode;
            config.scopes[0].extended_hours = true;
            let mut response = response(2000, 4000);
            response.scope.sessions = EligibleSessions::Extended;
            let store = Arc::new(
                TradingScheduleStore::load(config.clone(), pool)
                    .await
                    .unwrap(),
            );
            store
                .accept(
                    &config.scopes[0],
                    response,
                    DateTime::from_timestamp_millis(1000).unwrap(),
                )
                .await
                .unwrap();
            let policy = CloseFlattenPolicy::from_secs(1)
                .unwrap()
                .with_schedule(Some(store));
            let mut status = MarketSessionStatus::without_close_metadata(MarketSession::Extended);
            status.extended_session_closes_at = DateTime::from_timestamp_millis(4500);
            let now = DateTime::from_timestamp_millis(3500).unwrap();
            let expected = match mode {
                TradingScheduleMode::Observe => policy.active_window(status, now),
                TradingScheduleMode::Enabled => Some(CloseFlattenWindow {
                    started_at: DateTime::from_timestamp_millis(2000).unwrap(),
                    closes_at: DateTime::from_timestamp_millis(4000).unwrap(),
                }),
            };
            assert_eq!(
                policy.window_for(&Symbol::new("AAPL").unwrap(), status, now),
                expected
            );
        }
    }

    #[test]
    fn later_deadlines_never_relax_the_latch() {
        let initial = response(2000, 4000);
        let previous = merge_latch(None, initial.clone(), interval(&initial), 1000).unwrap();
        let later = response(3000, 5000);
        let merged = merge_latch(Some(previous), later.clone(), interval(&later), 1100).unwrap();
        assert_eq!(merged.interval.execution_cutoff.get(), 2000);
        assert_eq!(merged.interval.hedge_close.get(), 4000);
    }

    #[tokio::test]
    async fn closed_latch_survives_clock_rollback_but_not_a_verified_later_broker_session() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let config = config();
        let store = TradingScheduleStore::load(config.clone(), pool.clone())
            .await
            .unwrap();
        store
            .accept(
                &config.scopes[0],
                response(2000, 4000),
                DateTime::from_timestamp_millis(1000).unwrap(),
            )
            .await
            .unwrap();
        store
            .due_wakeups(DateTime::from_timestamp_millis(4000).unwrap())
            .await
            .unwrap();
        drop(store);
        let store = TradingScheduleStore::load(config, pool).await.unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        assert!(!store.allows_new_order(&symbol, DateTime::from_timestamp_millis(3000).unwrap()));
        let mut status = MarketSessionStatus::without_close_metadata(MarketSession::Regular);
        status.session_opens_at = DateTime::from_timestamp_millis(1000);
        status.regular_session_closes_at = DateTime::from_timestamp_millis(6000);
        store.observe_broker(&symbol, status).await.unwrap();
        assert!(!store.allows_new_order(&symbol, DateTime::from_timestamp_millis(5000).unwrap()));
        status.session_opens_at = DateTime::from_timestamp_millis(5000);
        store.observe_broker(&symbol, status).await.unwrap();
        assert!(store.allows_new_order(&symbol, DateTime::from_timestamp_millis(5000).unwrap()));
        assert_eq!(
            store.latches.read().unwrap()["regular"]
                .interval
                .hedge_close
                .get(),
            4000
        );
    }

    #[test]
    fn changed_interval_identity_cannot_reset_an_active_window() {
        let initial = response(2000, 4000);
        let previous = merge_latch(None, initial.clone(), interval(&initial), 1000).unwrap();
        let mut changed = interval(&initial);
        changed.id.0 = "replacement".into();
        assert!(matches!(
            merge_latch(Some(previous), initial, changed, 1100).unwrap_err(),
            TradingScheduleError::Overlap
        ));
    }

    #[test]
    fn revision_conflict_is_retained_with_the_earliest_boundary() {
        let initial = response(2000, 4000);
        let previous = merge_latch(None, initial.clone(), interval(&initial), 1000).unwrap();
        let mut earlier = response(1500, 3500);
        earlier.policy_revision.0 = "revision-two".into();
        let merged =
            merge_latch(Some(previous), earlier.clone(), interval(&earlier), 1100).unwrap();
        assert!(merged.conflicted);
        assert_eq!(merged.interval.execution_cutoff.get(), 1500);
        assert_eq!(merged.interval.hedge_close.get(), 3500);
    }
}
