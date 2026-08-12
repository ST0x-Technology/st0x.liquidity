//! Policy for aggressive hedging before a multi-day market closure.

use std::time::Duration;

use chrono::{DateTime, Utc};
use metrics::counter;
use thiserror::Error;
use tracing::error;

use st0x_execution::{CounterTradeSkipReason, MarketSession, MarketSessionStatus, PostCloseGap};

/// Shared close-flatten decision used by position scanning and hedge pricing.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CloseFlattenPolicy {
    window: chrono::Duration,
}

impl CloseFlattenPolicy {
    pub(crate) fn from_secs(window_secs: u64) -> Result<Self, chrono::OutOfRangeError> {
        chrono::Duration::from_std(Duration::from_secs(window_secs)).map(|window| Self { window })
    }

    #[must_use]
    pub(crate) fn active_window(
        self,
        status: MarketSessionStatus,
        now: DateTime<Utc>,
    ) -> Option<CloseFlattenWindow> {
        if status.session != MarketSession::Extended
            || status.post_close_gap == PostCloseGap::OrdinaryOvernight
        {
            return None;
        }

        let Some(closes_at) = status.extended_session_closes_at else {
            counter!(
                "close_flatten_blocked_total",
                "reason" => "close_time_unknown"
            )
            .increment(1);
            error!(
                post_close_gap = ?status.post_close_gap,
                "extended session close time unknown; skipping close-flattening for this \
                 non-ordinary post-close gap"
            );
            return None;
        };
        let started_at = closes_at - self.window;

        (now >= started_at && now < closes_at).then_some(CloseFlattenWindow {
            started_at,
            closes_at,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CloseFlattenWindow {
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) closes_at: DateTime<Utc>,
}

/// How far a close-flatten hedge crosses its resolved reference price, ramped
/// linearly from `base_bps` at the window's start to `max_bps` at the
/// extended-session close.
///
/// Anchored to the window rather than to a per-order attempt count, so the cross
/// is a pure function of `(window, now)`: restart-safe, identical across apalis
/// retries, and independent of how many reprice cycles actually land. A position
/// that first becomes ready mid-window opens partway up the ramp, which is the
/// intent -- it has less time left to flatten (ADR 0019).
#[derive(Debug, Clone, Copy)]
pub(crate) struct CloseFlattenCrossRamp {
    base_bps: u16,
    max_bps: u16,
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub(crate) enum CloseFlattenCrossRampError {
    #[error("close-flatten cross ceiling {max_bps} bps is below the slippage base {base_bps} bps")]
    CeilingBelowBase { base_bps: u16, max_bps: u16 },
}

impl CloseFlattenCrossRamp {
    pub(crate) const fn new(
        base_bps: u16,
        max_bps: u16,
    ) -> Result<Self, CloseFlattenCrossRampError> {
        if max_bps < base_bps {
            return Err(CloseFlattenCrossRampError::CeilingBelowBase { base_bps, max_bps });
        }

        Ok(Self { base_bps, max_bps })
    }

    /// The cross an extended-hours hedge takes: the base band when no
    /// close-flatten window is active, the ramped cross when one is.
    ///
    /// The ramp owns both halves because they are one quantity -- the base band
    /// is the ramp's first rung. Splitting them across a separate context field
    /// left two independently-settable values whose equality nothing enforced,
    /// on the number that prices every extended-hours hedge.
    #[must_use]
    pub(crate) fn cross_bps(self, window: Option<CloseFlattenWindow>, now: DateTime<Utc>) -> u16 {
        let Some(window) = window else {
            return self.base_bps;
        };

        let span = u64::from(self.max_bps - self.base_bps);
        let total = (window.closes_at - window.started_at).num_seconds();

        // A non-positive window has no ramp to walk. It cannot arise from
        // `active_window` (which only yields a window when `now < closes_at`),
        // but crossing at the ceiling is the safe reading of "no time left".
        if total <= 0 {
            return self.max_bps;
        }

        let elapsed = (now - window.started_at).num_seconds().clamp(0, total);
        let progressed = u128::from(self.base_bps)
            + u128::from(span) * u128::from(elapsed.unsigned_abs())
                / u128::from(total.unsigned_abs());

        // `span * elapsed / total <= span`, so `progressed <= max_bps` and the
        // conversion cannot narrow. The impossible arm is handled loudly rather
        // than capped behind an `unwrap_or`: the cross is the slippage budget a
        // live limit price is derived from, so a widened input must be visible
        // in the logs instead of silently pricing at the ceiling.
        match u16::try_from(progressed) {
            Ok(cross_bps) => cross_bps,
            Err(error) => {
                error!(
                    %error,
                    progressed,
                    base_bps = self.base_bps,
                    max_bps = self.max_bps,
                    "close-flatten cross exceeded the u16 basis-point domain; \
                     crossing at the configured ceiling"
                );
                self.max_bps
            }
        }
    }
}

/// Shared by `position_check.rs`'s scan-time preflight and `hedge.rs`'s
/// perform-time close-flatten re-preflight, which labels its own rejections
/// identically to the scan-time preflight.
pub(crate) fn preflight_skip_reason_label(reason: &CounterTradeSkipReason) -> &'static str {
    match reason {
        CounterTradeSkipReason::InsufficientEquity { .. } => "insufficient_equity",
        CounterTradeSkipReason::InsufficientBuyingPower { .. } => "insufficient_buying_power",
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use proptest::prelude::*;

    use super::*;

    fn status(post_close_gap: PostCloseGap, closes_at: DateTime<Utc>) -> MarketSessionStatus {
        MarketSessionStatus {
            session: MarketSession::Extended,
            extended_session_closes_at: Some(closes_at),
            post_close_gap,
        }
    }

    #[test]
    fn ordinary_overnight_never_activates_close_flattening() {
        let now = Utc::now();
        let policy = CloseFlattenPolicy::from_secs(900).unwrap();

        assert!(
            policy
                .active_window(
                    status(PostCloseGap::OrdinaryOvernight, now + TimeDelta::minutes(5)),
                    now,
                )
                .is_none()
        );
    }

    #[test]
    fn multi_day_and_unknown_gaps_activate_inside_window() {
        let now = Utc::now();
        let closes_at = now + TimeDelta::minutes(5);
        let policy = CloseFlattenPolicy::from_secs(900).unwrap();

        assert!(
            policy
                .active_window(status(PostCloseGap::MultiDayClosure, closes_at), now)
                .is_some()
        );
        assert!(
            policy
                .active_window(status(PostCloseGap::Unknown, closes_at), now)
                .is_some()
        );
    }

    #[test]
    fn long_gap_outside_window_does_not_activate() {
        let now = Utc::now();
        let policy = CloseFlattenPolicy::from_secs(900).unwrap();

        assert!(
            policy
                .active_window(
                    status(PostCloseGap::MultiDayClosure, now + TimeDelta::minutes(16)),
                    now,
                )
                .is_none()
        );
    }

    #[test]
    fn window_activates_exactly_at_started_at() {
        // Half-open window: `now >= started_at`, so the lower bound itself
        // must activate. Pins the left edge so a `>` regression (which would
        // miss the first instant of the window) is caught.
        let now = Utc::now();
        let policy = CloseFlattenPolicy::from_secs(900).unwrap();
        let closes_at = now + TimeDelta::seconds(900);

        let window = policy
            .active_window(status(PostCloseGap::MultiDayClosure, closes_at), now)
            .expect("now == started_at must activate the window");

        assert_eq!(window.started_at, now);
    }

    #[test]
    fn unknown_close_time_does_not_activate_close_flattening() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let now = Utc::now();
        let policy = CloseFlattenPolicy::from_secs(900).unwrap();
        let status = MarketSessionStatus {
            session: MarketSession::Extended,
            extended_session_closes_at: None,
            post_close_gap: PostCloseGap::MultiDayClosure,
        };

        assert_eq!(policy.active_window(status, now), None);

        let rendered = metrics_handle.render();
        assert!(rendered.contains("close_flatten_blocked_total{"));
        assert!(rendered.contains("reason=\"close_time_unknown\""));
    }

    #[test]
    fn window_does_not_activate_exactly_at_closes_at() {
        // Half-open window: `now < closes_at`, so the upper bound itself must
        // NOT activate. Pins the right edge so a `<=` regression (which would
        // extend aggressive crossing one tick past session close) is caught.
        let now = Utc::now();
        let policy = CloseFlattenPolicy::from_secs(900).unwrap();
        let closes_at = now;

        assert!(
            policy
                .active_window(status(PostCloseGap::MultiDayClosure, closes_at), now)
                .is_none(),
            "now == closes_at must not activate the window"
        );
    }

    /// A 15-minute window crossing from 1% to 4%, the production shape.
    fn ramp_window() -> (CloseFlattenCrossRamp, CloseFlattenWindow) {
        let started_at = "2026-08-07T23:45:00Z".parse::<DateTime<Utc>>().unwrap();
        (
            CloseFlattenCrossRamp::new(100, 400).unwrap(),
            CloseFlattenWindow {
                started_at,
                closes_at: started_at + chrono::Duration::seconds(900),
            },
        )
    }

    #[test]
    fn cross_starts_at_the_base_and_ends_at_the_ceiling() {
        let (ramp, window) = ramp_window();

        assert_eq!(ramp.cross_bps(Some(window), window.started_at), 100);
        assert_eq!(ramp.cross_bps(Some(window), window.closes_at), 400);
    }

    /// Outside a window the ramp yields its base band, so an ordinary
    /// extended-hours hedge and the ramp's first rung can never drift apart.
    #[test]
    fn cross_is_the_base_band_without_a_window() {
        let (ramp, _) = ramp_window();

        assert_eq!(ramp.cross_bps(None, Utc::now()), 100);
    }

    #[test]
    fn cross_ramps_linearly_across_the_window() {
        let (ramp, window) = ramp_window();

        for (elapsed_secs, expected) in [(225, 175), (450, 250), (675, 325)] {
            let now = window.started_at + chrono::Duration::seconds(elapsed_secs);
            assert_eq!(
                ramp.cross_bps(Some(window), now),
                expected,
                "{elapsed_secs}s into a 900s window must cross {expected} bps"
            );
        }
    }

    /// The cross must never exceed the configured ceiling, whatever the clock
    /// says -- a reprice can land after `closes_at` if the session check and the
    /// pricing call straddle the close.
    #[test]
    fn cross_is_clamped_past_the_close() {
        let (ramp, window) = ramp_window();
        let past_close = window.closes_at + chrono::Duration::hours(3);

        assert_eq!(ramp.cross_bps(Some(window), past_close), 400);
    }

    /// A clock reading before the window opened must not underflow into a
    /// negative elapsed and price below the base.
    #[test]
    fn cross_is_clamped_before_the_window_opens() {
        let (ramp, window) = ramp_window();
        let before = window.started_at - chrono::Duration::hours(1);

        assert_eq!(ramp.cross_bps(Some(window), before), 100);
    }

    /// Equal base and ceiling degenerates to the existing flat band rather than
    /// dividing by a zero span.
    #[test]
    fn cross_is_flat_when_base_equals_the_ceiling() {
        let (_, window) = ramp_window();
        let ramp = CloseFlattenCrossRamp::new(100, 100).unwrap();

        assert_eq!(ramp.cross_bps(Some(window), window.started_at), 100);
        assert_eq!(ramp.cross_bps(Some(window), window.closes_at), 100);
    }

    #[test]
    fn inverted_cross_ramp_is_rejected() {
        assert_eq!(
            CloseFlattenCrossRamp::new(400, 100).unwrap_err(),
            CloseFlattenCrossRampError::CeilingBelowBase {
                base_bps: 400,
                max_bps: 100,
            }
        );
    }

    #[test]
    fn cross_returns_the_ceiling_for_a_zero_length_window() {
        let started_at = "2026-08-07T23:45:00Z".parse::<DateTime<Utc>>().unwrap();
        let window = CloseFlattenWindow {
            started_at,
            closes_at: started_at,
        };

        assert_eq!(
            CloseFlattenCrossRamp::new(100, 400)
                .unwrap()
                .cross_bps(Some(window), started_at),
            400
        );
    }

    #[test]
    fn cross_handles_the_largest_representable_window_without_overflow() {
        let started_at = DateTime::<Utc>::MIN_UTC;
        let window = CloseFlattenWindow {
            started_at,
            closes_at: DateTime::<Utc>::MAX_UTC,
        };
        let halfway = started_at + (window.closes_at - started_at) / 2;

        assert_eq!(
            CloseFlattenCrossRamp::new(1, 9_999)
                .unwrap()
                .cross_bps(Some(window), halfway),
            4_999,
            "integer interpolation must round down without overflowing at the largest DateTime window"
        );
    }

    proptest! {
        #[test]
        fn cross_is_bounded_and_monotonic(
            first_bps in any::<u16>(),
            second_bps in any::<u16>(),
            first_offset_secs in -1_800i64..=2_700,
            second_offset_secs in -1_800i64..=2_700,
        ) {
            let base_bps = first_bps.min(second_bps);
            let max_bps = first_bps.max(second_bps);
            let ramp = CloseFlattenCrossRamp::new(base_bps, max_bps).unwrap();
            let started_at = "2026-08-07T23:45:00Z".parse::<DateTime<Utc>>().unwrap();
            let window = CloseFlattenWindow {
                started_at,
                closes_at: started_at + chrono::Duration::seconds(900),
            };
            let earlier_offset = first_offset_secs.min(second_offset_secs);
            let later_offset = first_offset_secs.max(second_offset_secs);
            let earlier = ramp.cross_bps(
                Some(window),
                started_at + chrono::Duration::seconds(earlier_offset),
            );
            let later = ramp.cross_bps(
                Some(window),
                started_at + chrono::Duration::seconds(later_offset),
            );

            prop_assert!((base_bps..=max_bps).contains(&earlier));
            prop_assert!((base_bps..=max_bps).contains(&later));
            prop_assert!(earlier <= later);
        }
    }
}
