//! Fail-closed overnight eligibility: the last synced asset-attribute
//! snapshot per symbol, and the validation the hedge path and the CLI
//! run before an overnight placement.
//!
//! Eligibility is decided from a SNAPSHOT, never a live fetch: the
//! scheduled 19:55 ET sync (conductor task) refreshes attributes right
//! before the session opens, and every decision paths through
//! [`validate_overnight_eligibility`] so a missing, stale, or negative
//! snapshot rejects the order instead of guessing.

use chrono::{DateTime, Days, LocalResult, NaiveDate, NaiveTime, TimeDelta, TimeZone, Utc};
use chrono_tz::America::New_York;
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};
use tracing::warn;

use super::{AssetDetails, AssetStatus};
use crate::Symbol;

/// 19:45 ET: the sync window preceding a session's 20:00 open starts.
/// Const-evaluated, so an invalid literal fails the build, not the run.
const SYNC_WINDOW_OPEN_ET: NaiveTime = match NaiveTime::from_hms_opt(19, 45, 0) {
    Some(time) => time,
    None => panic!("19:45:00 is a valid wall-clock time"),
};

/// 19:55 ET: the daily sync slot inside the window, five minutes before
/// the session opens.
const SYNC_SLOT_ET: NaiveTime = match NaiveTime::from_hms_opt(19, 55, 0) {
    Some(time) => time,
    None => panic!("19:55:00 is a valid wall-clock time"),
};

/// One symbol's asset attributes as of the last successful sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EligibilitySnapshot {
    pub synced_at: DateTime<Utc>,
    pub details: AssetDetails,
}

/// The shared per-symbol snapshot store the scheduled sync writes and
/// eligibility decisions read. Cloning shares the same store.
#[derive(Clone, Default)]
pub struct EligibilitySnapshots {
    inner: Arc<RwLock<HashMap<Symbol, EligibilitySnapshot>>>,
}

impl EligibilitySnapshots {
    /// The symbol's last recorded snapshot, copied out so no lock is
    /// held across the caller's work.
    pub fn get(&self, symbol: &Symbol) -> Option<EligibilitySnapshot> {
        // A panic while holding this lock cannot corrupt the map (it
        // only ever holds fully-formed snapshots), so recover from
        // poison instead of cascading the panic.
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(symbol)
            .copied()
    }

    /// Records a snapshot. Production writes go through
    /// [`sync_eligibility`]; public so consuming crates' tests can seed
    /// the store directly.
    pub fn record(&self, symbol: Symbol, snapshot: EligibilitySnapshot) {
        self.inner
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(symbol, snapshot);
    }
}

/// A sync run that could not refresh every configured symbol. Successful
/// symbols are already recorded; `failures` carries the rest so the
/// caller can alert with the exact scope of the gap.
#[derive(Debug, thiserror::Error)]
#[error("eligibility sync failed for {} symbol(s)", failures.len())]
pub struct EligibilitySyncError {
    pub failures: Vec<(Symbol, super::AlpacaBrokerApiError)>,
}

/// Refreshes every configured symbol's asset attributes from the broker
/// (bypassing the placement-side TTL cache) and records a snapshot per
/// success, stamped at fetch completion.
///
/// Serial on purpose: the configured asset count is small and serial keeps
/// the failure attribution simple. Failures leave the previous snapshot in
/// place -- the session-scoped staleness check is what retires it.
pub async fn sync_eligibility(
    broker: &super::AlpacaBrokerApi,
    symbols: &[Symbol],
    store: &EligibilitySnapshots,
) -> Result<(), EligibilitySyncError> {
    let mut failures = Vec::new();
    for symbol in symbols {
        match broker.refresh_asset_details(symbol).await {
            Ok(details) => store.record(
                symbol.clone(),
                EligibilitySnapshot {
                    synced_at: Utc::now(),
                    details,
                },
            ),
            Err(error) => {
                warn!(%symbol, ?error, "Asset eligibility refresh failed; keeping the previous snapshot");
                failures.push((symbol.clone(), error));
            }
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(EligibilitySyncError { failures })
    }
}

/// The quantity shape of the order being validated: the fractional
/// matrix only constrains fractional quantities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OvernightOrderShape {
    WholeShares,
    Fractional,
}

/// Why an overnight placement is refused. Every variant is fail-closed:
/// unknown state rejects exactly like negative state.
#[derive(Debug, thiserror::Error)]
pub enum OvernightEligibilityError {
    #[error("no eligibility snapshot for {symbol}: the asset sync has not run")]
    NoSnapshot { symbol: Symbol },
    #[error(
        "eligibility snapshot for {symbol} is stale (synced at {synced_at}); \
         refusing to place from outdated attributes"
    )]
    StaleSnapshot {
        symbol: Symbol,
        synced_at: DateTime<Utc>,
    },
    #[error("{symbol} is not an active tradable asset at the broker")]
    NotTradable { symbol: Symbol },
    #[error("{symbol} is not overnight tradable")]
    NotOvernightTradable { symbol: Symbol },
    #[error("{symbol} is halted for the overnight session")]
    OvernightHalted { symbol: Symbol },
    #[error(
        "fractional orders for {symbol} are not eligible overnight \
         (requires fractionable and fractional_eh_enabled)"
    )]
    FractionalNotEligible { symbol: Symbol },
}

/// Validates an overnight placement against the symbol's last synced
/// snapshot.
///
/// Fail-closed: no snapshot, a stale snapshot, or any attribute that is
/// absent (`None`) or negative refuses the order with a typed error.
///
/// Staleness follows the spec's session-scoped rule, not a duration:
/// attributes authorize placement only for the overnight session they
/// were synced for, so the snapshot must have completed at or after the
/// start of the 19:45 ET sync window preceding that session's 20:00
/// open (see [`eligibility_sync_window_start`]). A startup sync
/// satisfies this when the bot starts mid-session.
pub fn validate_overnight_eligibility(
    symbol: &Symbol,
    snapshot: Option<&EligibilitySnapshot>,
    shape: OvernightOrderShape,
    now: DateTime<Utc>,
) -> Result<(), OvernightEligibilityError> {
    let Some(snapshot) = snapshot else {
        return Err(OvernightEligibilityError::NoSnapshot {
            symbol: symbol.clone(),
        });
    };

    if snapshot.synced_at < eligibility_sync_window_start(now) {
        return Err(OvernightEligibilityError::StaleSnapshot {
            symbol: symbol.clone(),
            synced_at: snapshot.synced_at,
        });
    }

    let details = snapshot.details;
    match details.status {
        AssetStatus::Active => {}
        AssetStatus::Inactive => {
            return Err(OvernightEligibilityError::NotTradable {
                symbol: symbol.clone(),
            });
        }
    }
    if !details.tradable {
        return Err(OvernightEligibilityError::NotTradable {
            symbol: symbol.clone(),
        });
    }

    if details.overnight_tradable != Some(true) {
        return Err(OvernightEligibilityError::NotOvernightTradable {
            symbol: symbol.clone(),
        });
    }
    if details.overnight_halted != Some(false) {
        return Err(OvernightEligibilityError::OvernightHalted {
            symbol: symbol.clone(),
        });
    }

    match shape {
        OvernightOrderShape::WholeShares => Ok(()),
        OvernightOrderShape::Fractional => {
            if details.fractionable == Some(true) && details.fractional_eh_enabled == Some(true) {
                Ok(())
            } else {
                Err(OvernightEligibilityError::FractionalNotEligible {
                    symbol: symbol.clone(),
                })
            }
        }
    }
}

/// The start of the 19:45 ET sync window whose overnight session covers
/// `now`: the most recent 19:45 America/New_York instant at or before
/// `now`.
///
/// During an evening leg (20:00-24:00 ET) that is the same evening's
/// 19:45; during a morning leg (00:00-04:00 ET) it is the PREVIOUS
/// evening's 19:45 -- both legs belong to the session that opened at
/// the preceding 20:00.
pub fn eligibility_sync_window_start(now: DateTime<Utc>) -> DateTime<Utc> {
    let today_et = now.with_timezone(&New_York).date_naive();
    let todays_window = et_wall_clock_as_utc(today_et, SYNC_WINDOW_OPEN_ET);
    if todays_window <= now {
        return todays_window;
    }

    let Some(yesterday) = today_et.checked_sub_days(Days::new(1)) else {
        // Calendar underflow is unreachable for real clocks; pin to the
        // only representable window instead of panicking.
        warn!(%today_et, "Could not compute the previous calendar day");
        return todays_window;
    };
    et_wall_clock_as_utc(yesterday, SYNC_WINDOW_OPEN_ET)
}

/// The next 19:55 America/New_York instant strictly after `now`: the
/// daily eligibility-sync slot, five minutes before the overnight
/// session opens.
///
/// The slot is a fixed ET wall-clock time (like the session boundaries
/// in `market_hours`), so the UTC instant shifts with DST and the math
/// lives in `America/New_York`, never in UTC offsets.
///
/// Strictly after: a caller asking at exactly 19:55 has already fired
/// this slot and gets tomorrow's, so a sync loop can never double-fire
/// one slot or busy-spin on the boundary instant.
pub fn next_eligibility_sync_at(now: DateTime<Utc>) -> DateTime<Utc> {
    let today_et = now.with_timezone(&New_York).date_naive();
    let todays_slot = et_wall_clock_as_utc(today_et, SYNC_SLOT_ET);
    if todays_slot > now {
        return todays_slot;
    }

    let Some(tomorrow) = today_et.checked_add_days(Days::new(1)) else {
        // Calendar overflow is unreachable for real clocks; pin to the
        // last representable slot instead of panicking.
        warn!(%today_et, "Could not compute the next calendar day");
        return todays_slot;
    };
    et_wall_clock_as_utc(tomorrow, SYNC_SLOT_ET)
}

/// The UTC instant of an ET wall-clock time on a date. The fixed 19:xx
/// slots never fall in the 01:00-03:00 local DST transition windows, so
/// resolution is always `Single`; the other arms resolve
/// deterministically (with a warning) instead of panicking in case the
/// zone rules ever shift.
fn et_wall_clock_as_utc(date: NaiveDate, time: NaiveTime) -> DateTime<Utc> {
    let naive = date.and_time(time);
    match naive.and_local_timezone(New_York) {
        LocalResult::Single(instant) => instant.with_timezone(&Utc),
        LocalResult::Ambiguous(earliest, _) => {
            warn!(%naive, "ET wall-clock time is ambiguous; using the earlier instant");
            earliest.with_timezone(&Utc)
        }
        LocalResult::None => {
            // A spring-forward gap: the EST offset (UTC-5) is the
            // deterministic stand-in.
            warn!(%naive, "ET wall-clock time falls in a DST gap; using the EST offset");
            Utc.from_utc_datetime(&(naive + TimeDelta::hours(5)))
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use httpmock::MockServer;
    use uuid::uuid;

    use super::super::{AlpacaAccountId, AssetStatus};
    use super::*;
    use crate::Executor;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    /// 2026-08-28 21:00 EDT: one hour into the evening leg of an
    /// overnight session. Its sync window opened at 19:45 EDT = 23:45 UTC.
    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 29, 1, 0, 0).unwrap()
    }

    fn window_start() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 28, 23, 45, 0).unwrap()
    }

    fn rklb() -> Symbol {
        Symbol::new("RKLB").unwrap()
    }

    /// A fully eligible snapshot from the session's own 19:55 ET sync.
    fn eligible_snapshot() -> EligibilitySnapshot {
        EligibilitySnapshot {
            synced_at: Utc.with_ymd_and_hms(2026, 8, 28, 23, 55, 0).unwrap(),
            details: AssetDetails {
                status: AssetStatus::Active,
                tradable: true,
                fractionable: Some(true),
                fractional_eh_enabled: Some(true),
                overnight_tradable: Some(true),
                overnight_halted: Some(false),
            },
        }
    }

    fn validate(
        snapshot: Option<&EligibilitySnapshot>,
        shape: OvernightOrderShape,
    ) -> Result<(), OvernightEligibilityError> {
        validate_overnight_eligibility(&rklb(), snapshot, shape, now())
    }

    #[test]
    fn whole_share_order_on_a_fully_eligible_snapshot_passes() {
        validate(Some(&eligible_snapshot()), OvernightOrderShape::WholeShares).unwrap();
    }

    #[test]
    fn fractional_order_on_a_fully_eligible_snapshot_passes() {
        validate(Some(&eligible_snapshot()), OvernightOrderShape::Fractional).unwrap();
    }

    #[test]
    fn missing_snapshot_fails_closed() {
        let error = validate(None, OvernightOrderShape::WholeShares).unwrap_err();
        assert!(
            matches!(error, OvernightEligibilityError::NoSnapshot { ref symbol } if *symbol == rklb()),
            "expected NoSnapshot, got {error:?}"
        );
    }

    #[test]
    fn snapshot_from_before_the_sync_window_fails_closed() {
        // Synced at 11:00 EDT the same day: attributes from before the
        // session's 19:45 window authorize nothing for this session.
        let mut snapshot = eligible_snapshot();
        snapshot.synced_at = Utc.with_ymd_and_hms(2026, 8, 28, 15, 0, 0).unwrap();

        let error = validate(Some(&snapshot), OvernightOrderShape::WholeShares).unwrap_err();
        assert!(
            matches!(
                error,
                OvernightEligibilityError::StaleSnapshot { ref symbol, synced_at }
                    if *symbol == rklb() && synced_at == snapshot.synced_at
            ),
            "expected StaleSnapshot, got {error:?}"
        );
    }

    #[test]
    fn snapshot_exactly_at_the_window_start_still_passes() {
        // The spec bound is "at or after the start of the 19:45 ET
        // window": the boundary instant itself authorizes.
        let mut snapshot = eligible_snapshot();
        snapshot.synced_at = window_start();

        validate(Some(&snapshot), OvernightOrderShape::WholeShares).unwrap();
    }

    #[test]
    fn morning_leg_accepts_the_previous_evenings_sync() {
        // 02:00 EDT on 08-29 (06:00 UTC) is the morning leg of the
        // session that opened 08-28 20:00 EDT: the 08-28 19:55 sync
        // still authorizes it.
        let morning = Utc.with_ymd_and_hms(2026, 8, 29, 6, 0, 0).unwrap();

        validate_overnight_eligibility(
            &rklb(),
            Some(&eligible_snapshot()),
            OvernightOrderShape::WholeShares,
            morning,
        )
        .unwrap();
    }

    #[test]
    fn morning_leg_rejects_a_sync_from_before_the_previous_evening() {
        // Synced 08-28 at 11:00 EDT, asked on the 08-29 morning leg:
        // still the same stale-window refusal.
        let mut snapshot = eligible_snapshot();
        snapshot.synced_at = Utc.with_ymd_and_hms(2026, 8, 28, 15, 0, 0).unwrap();
        let morning = Utc.with_ymd_and_hms(2026, 8, 29, 6, 0, 0).unwrap();

        let error = validate_overnight_eligibility(
            &rklb(),
            Some(&snapshot),
            OvernightOrderShape::WholeShares,
            morning,
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                OvernightEligibilityError::StaleSnapshot { ref symbol, .. } if *symbol == rklb()
            ),
            "expected StaleSnapshot, got {error:?}"
        );
    }

    #[test]
    fn mid_session_startup_sync_authorizes_the_running_session() {
        // The bot starting at 22:30 EDT syncs immediately; that sync is
        // inside the session's window and authorizes placements.
        let mut snapshot = eligible_snapshot();
        snapshot.synced_at = Utc.with_ymd_and_hms(2026, 8, 29, 2, 30, 0).unwrap();

        validate(Some(&snapshot), OvernightOrderShape::WholeShares).unwrap();
    }

    #[test]
    fn inactive_asset_fails_closed() {
        let mut snapshot = eligible_snapshot();
        snapshot.details.status = AssetStatus::Inactive;

        let error = validate(Some(&snapshot), OvernightOrderShape::WholeShares).unwrap_err();
        assert!(
            matches!(error, OvernightEligibilityError::NotTradable { ref symbol } if *symbol == rklb()),
            "expected NotTradable, got {error:?}"
        );
    }

    #[test]
    fn untradable_asset_fails_closed() {
        let mut snapshot = eligible_snapshot();
        snapshot.details.tradable = false;

        let error = validate(Some(&snapshot), OvernightOrderShape::WholeShares).unwrap_err();
        assert!(
            matches!(error, OvernightEligibilityError::NotTradable { ref symbol } if *symbol == rklb()),
            "expected NotTradable, got {error:?}"
        );
    }

    #[test]
    fn overnight_tradable_false_fails_closed() {
        let mut snapshot = eligible_snapshot();
        snapshot.details.overnight_tradable = Some(false);

        let error = validate(Some(&snapshot), OvernightOrderShape::WholeShares).unwrap_err();
        assert!(
            matches!(
                error,
                OvernightEligibilityError::NotOvernightTradable { ref symbol } if *symbol == rklb()
            ),
            "expected NotOvernightTradable, got {error:?}"
        );
    }

    #[test]
    fn absent_overnight_tradable_attribute_fails_closed() {
        // The broker omitted the attributes array entirely: unknown state
        // must reject exactly like a negative one.
        let mut snapshot = eligible_snapshot();
        snapshot.details.overnight_tradable = None;

        let error = validate(Some(&snapshot), OvernightOrderShape::WholeShares).unwrap_err();
        assert!(
            matches!(
                error,
                OvernightEligibilityError::NotOvernightTradable { ref symbol } if *symbol == rklb()
            ),
            "expected NotOvernightTradable, got {error:?}"
        );
    }

    #[test]
    fn halted_asset_fails_closed() {
        let mut snapshot = eligible_snapshot();
        snapshot.details.overnight_halted = Some(true);

        let error = validate(Some(&snapshot), OvernightOrderShape::WholeShares).unwrap_err();
        assert!(
            matches!(
                error,
                OvernightEligibilityError::OvernightHalted { ref symbol } if *symbol == rklb()
            ),
            "expected OvernightHalted, got {error:?}"
        );
    }

    #[test]
    fn absent_halted_attribute_fails_closed() {
        let mut snapshot = eligible_snapshot();
        snapshot.details.overnight_halted = None;

        let error = validate(Some(&snapshot), OvernightOrderShape::WholeShares).unwrap_err();
        assert!(
            matches!(
                error,
                OvernightEligibilityError::OvernightHalted { ref symbol } if *symbol == rklb()
            ),
            "expected OvernightHalted, got {error:?}"
        );
    }

    #[test]
    fn fractional_order_without_fractional_eh_fails_closed() {
        for fractional_eh_enabled in [Some(false), None] {
            let mut snapshot = eligible_snapshot();
            snapshot.details.fractional_eh_enabled = fractional_eh_enabled;

            let error = validate(Some(&snapshot), OvernightOrderShape::Fractional).unwrap_err();
            assert!(
                matches!(
                    error,
                    OvernightEligibilityError::FractionalNotEligible { ref symbol }
                        if *symbol == rklb()
                ),
                "expected FractionalNotEligible for {fractional_eh_enabled:?}, got {error:?}"
            );
        }
    }

    #[test]
    fn fractional_order_without_fractionable_fails_closed() {
        for fractionable in [Some(false), None] {
            let mut snapshot = eligible_snapshot();
            snapshot.details.fractionable = fractionable;

            let error = validate(Some(&snapshot), OvernightOrderShape::Fractional).unwrap_err();
            assert!(
                matches!(
                    error,
                    OvernightEligibilityError::FractionalNotEligible { ref symbol }
                        if *symbol == rklb()
                ),
                "expected FractionalNotEligible for {fractionable:?}, got {error:?}"
            );
        }
    }

    #[test]
    fn whole_share_order_ignores_the_fractional_matrix() {
        // Whole shares stay allowed on a non-fractionable asset: only the
        // overnight attributes gate them.
        let mut snapshot = eligible_snapshot();
        snapshot.details.fractionable = Some(false);
        snapshot.details.fractional_eh_enabled = None;

        validate(Some(&snapshot), OvernightOrderShape::WholeShares).unwrap();
    }

    fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .unwrap()
    }

    #[test]
    fn evening_leg_window_starts_at_the_same_evenings_1945() {
        // 21:00 EDT -> that evening's 19:45 EDT (23:45 UTC).
        assert_eq!(
            eligibility_sync_window_start(utc(2026, 8, 29, 1, 0)),
            utc(2026, 8, 28, 23, 45)
        );
    }

    #[test]
    fn morning_leg_window_starts_at_the_previous_evenings_1945() {
        // 02:00 EDT on 08-29 belongs to the session opened 08-28 20:00.
        assert_eq!(
            eligibility_sync_window_start(utc(2026, 8, 29, 6, 0)),
            utc(2026, 8, 28, 23, 45)
        );
    }

    #[test]
    fn winter_morning_leg_window_uses_the_est_offset() {
        // 02:00 EST on 12-11 (07:00 UTC) -> 12-10 19:45 EST = 00:45 UTC 12-11.
        assert_eq!(
            eligibility_sync_window_start(utc(2026, 12, 11, 7, 0)),
            utc(2026, 12, 11, 0, 45)
        );
    }

    #[test]
    fn morning_schedules_the_same_evening_sync() {
        // 2026-08-28 10:00 EDT (14:00 UTC) -> 19:55 EDT = 23:55 UTC.
        assert_eq!(
            next_eligibility_sync_at(utc(2026, 8, 28, 14, 0)),
            utc(2026, 8, 28, 23, 55)
        );
    }

    #[test]
    fn after_the_slot_schedules_the_next_day() {
        // 2026-08-28 20:30 EDT (00:30 UTC next day) -> 2026-08-29 19:55 EDT.
        assert_eq!(
            next_eligibility_sync_at(utc(2026, 8, 29, 0, 30)),
            utc(2026, 8, 29, 23, 55)
        );
    }

    #[test]
    fn exactly_at_the_slot_schedules_the_next_day() {
        // The boundary instant belongs to the slot that just fired.
        assert_eq!(
            next_eligibility_sync_at(utc(2026, 8, 28, 23, 55)),
            utc(2026, 8, 29, 23, 55)
        );
    }

    #[test]
    fn one_second_before_the_slot_keeps_the_same_day() {
        assert_eq!(
            next_eligibility_sync_at(Utc.with_ymd_and_hms(2026, 8, 28, 23, 54, 59).unwrap()),
            utc(2026, 8, 28, 23, 55)
        );
    }

    #[test]
    fn winter_slot_is_an_hour_later_in_utc() {
        // 2026-12-10 10:00 EST (15:00 UTC) -> 19:55 EST = 00:55 UTC next day.
        assert_eq!(
            next_eligibility_sync_at(utc(2026, 12, 10, 15, 0)),
            utc(2026, 12, 11, 0, 55)
        );
    }

    #[test]
    fn spring_forward_day_uses_the_edt_slot() {
        // DST starts 2026-03-08 02:00 ET. By 19:55 that evening the zone
        // is EDT, so the slot is 23:55 UTC, not 00:55 UTC.
        assert_eq!(
            next_eligibility_sync_at(utc(2026, 3, 8, 12, 0)),
            utc(2026, 3, 8, 23, 55)
        );
    }

    #[test]
    fn fall_back_day_uses_the_est_slot() {
        // DST ends 2026-11-01 02:00 ET. By 19:55 that evening the zone is
        // EST, so the slot is 00:55 UTC on the next calendar day.
        assert_eq!(
            next_eligibility_sync_at(utc(2026, 11, 1, 14, 0)),
            utc(2026, 11, 2, 0, 55)
        );
    }

    #[test]
    fn crossing_the_spring_forward_night_lands_on_the_edt_slot() {
        // Asked late on the EST side of the spring-forward night (01:00
        // UTC on 03-08 is still 20:00 EST 03-07), the next slot is 03-08
        // 19:55 EDT = 23:55 UTC -- 23 wall-clock hours later, not 24.
        assert_eq!(
            next_eligibility_sync_at(utc(2026, 3, 8, 1, 0)),
            utc(2026, 3, 8, 23, 55)
        );
    }

    fn mock_ctx(server: &MockServer) -> super::super::AlpacaBrokerApiCtx {
        super::super::AlpacaBrokerApiCtx {
            auth: crate::AlpacaBrokerAuth::Basic {
                api_key: "test_key".to_string(),
                api_secret: "test_secret".to_string(),
            },
            account_id: TEST_ACCOUNT_ID,
            mode: Some(super::super::AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: crate::TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        }
    }

    fn mock_account(server: &MockServer) {
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE"
                }));
        });
    }

    fn mock_asset<'server>(
        server: &'server MockServer,
        symbol: &str,
        attributes: &serde_json::Value,
    ) -> httpmock::Mock<'server> {
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!("/v1/assets/{symbol}"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": symbol,
                    "status": "active",
                    "tradable": true,
                    "fractionable": true,
                    "attributes": attributes
                }));
        })
    }

    async fn mock_broker(server: &MockServer) -> super::super::AlpacaBrokerApi {
        super::super::AlpacaBrokerApi::try_from_ctx(mock_ctx(server))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn sync_records_a_fresh_snapshot_for_every_configured_symbol() {
        let server = MockServer::start_async().await;
        mock_account(&server);
        mock_asset(
            &server,
            "AAPL",
            &serde_json::json!(["fractional_eh_enabled", "overnight_tradable"]),
        );
        mock_asset(&server, "RKLB", &serde_json::json!([]));
        let broker = mock_broker(&server).await;
        let store = EligibilitySnapshots::default();
        let started_at = Utc::now();

        sync_eligibility(
            &broker,
            &[Symbol::new("AAPL").unwrap(), Symbol::new("RKLB").unwrap()],
            &store,
        )
        .await
        .unwrap();

        let aapl = store.get(&Symbol::new("AAPL").unwrap()).unwrap();
        assert_eq!(
            aapl.details,
            AssetDetails {
                status: AssetStatus::Active,
                tradable: true,
                fractionable: Some(true),
                fractional_eh_enabled: Some(true),
                overnight_tradable: Some(true),
                overnight_halted: Some(false),
            }
        );
        assert!(aapl.synced_at >= started_at);

        let rklb = store.get(&Symbol::new("RKLB").unwrap()).unwrap();
        assert_eq!(
            rklb.details,
            AssetDetails {
                status: AssetStatus::Active,
                tradable: true,
                fractionable: Some(true),
                fractional_eh_enabled: Some(false),
                overnight_tradable: Some(false),
                overnight_halted: Some(false),
            }
        );
    }

    #[tokio::test]
    async fn sync_bypasses_the_placement_asset_cache() {
        // Prime the executor's TTL cache with pre-window attributes, then
        // change the broker payload: the sync must record the NEW
        // attributes, not serve the one-hour cache.
        let server = MockServer::start_async().await;
        mock_account(&server);
        let mut stale_asset =
            mock_asset(&server, "AAPL", &serde_json::json!(["overnight_tradable"]));
        let broker = mock_broker(&server).await;
        broker
            .get_asset_details(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap();

        stale_asset.delete();
        mock_asset(
            &server,
            "AAPL",
            &serde_json::json!(["overnight_tradable", "overnight_halted"]),
        );
        let store = EligibilitySnapshots::default();
        sync_eligibility(&broker, &[Symbol::new("AAPL").unwrap()], &store)
            .await
            .unwrap();

        let snapshot = store.get(&Symbol::new("AAPL").unwrap()).unwrap();
        assert_eq!(snapshot.details.overnight_halted, Some(true));
    }

    #[tokio::test]
    async fn sync_reports_failures_and_keeps_the_previous_snapshot() {
        let server = MockServer::start_async().await;
        mock_account(&server);
        let mut healthy_asset =
            mock_asset(&server, "AAPL", &serde_json::json!(["overnight_tradable"]));
        let broker = mock_broker(&server).await;
        let store = EligibilitySnapshots::default();
        sync_eligibility(&broker, &[Symbol::new("AAPL").unwrap()], &store)
            .await
            .unwrap();
        let first = store.get(&Symbol::new("AAPL").unwrap()).unwrap();

        healthy_asset.delete();
        server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/v1/assets/AAPL");
            then.status(500).body("broker exploded");
        });

        let error = sync_eligibility(&broker, &[Symbol::new("AAPL").unwrap()], &store)
            .await
            .unwrap_err();
        let [(ref failed_symbol, _)] = error.failures[..] else {
            panic!("expected exactly one failure, got {:?}", error.failures);
        };
        assert_eq!(*failed_symbol, Symbol::new("AAPL").unwrap());

        // The previous snapshot survives; the session-window staleness
        // check is what retires it, not the failed refresh.
        let kept = store.get(&Symbol::new("AAPL").unwrap()).unwrap();
        assert_eq!(kept.synced_at, first.synced_at);
        assert_eq!(kept.details, first.details);
    }

    #[tokio::test]
    async fn sync_records_successes_even_when_another_symbol_fails() {
        let server = MockServer::start_async().await;
        mock_account(&server);
        mock_asset(&server, "AAPL", &serde_json::json!(["overnight_tradable"]));
        server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/v1/assets/RKLB");
            then.status(500).body("broker exploded");
        });
        let broker = mock_broker(&server).await;
        let store = EligibilitySnapshots::default();

        let error = sync_eligibility(
            &broker,
            &[Symbol::new("AAPL").unwrap(), Symbol::new("RKLB").unwrap()],
            &store,
        )
        .await
        .unwrap_err();

        let [(ref failed_symbol, _)] = error.failures[..] else {
            panic!("expected exactly one failure, got {:?}", error.failures);
        };
        assert_eq!(*failed_symbol, Symbol::new("RKLB").unwrap());
        assert_eq!(
            store
                .get(&Symbol::new("AAPL").unwrap())
                .unwrap()
                .details
                .overnight_tradable,
            Some(true)
        );
        assert_eq!(store.get(&Symbol::new("RKLB").unwrap()), None);
    }
}
