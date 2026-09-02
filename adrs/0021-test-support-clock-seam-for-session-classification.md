# ADR 0021: Test-support clock seam for session classification

- Status: Proposed
- Date: 2026-09-02

## Context

RAI-1954 must cover overnight hedging with end-to-end tests: real conductor,
real queues, real event store, mock broker. The overnight session (20:00–04:00
ET) is anchored to the wall clock by Alpaca's 24/5 contract, and the classifier
implements it that way: after the calendar-driven Regular and Extended checks,
the overnight legs test the literal hour of the classification instant
(`hour >= 20 || hour < 4` in ET, `market_hours.rs`). No calendar field describes
the overnight span, so no mock fixture can move it.

The other three sessions are already fabricatable at any real time: the calendar
span takes precedence over the overnight hour-legs, so the mock can stretch
`session_open`/`session_close` to produce Regular or Extended at will (the
classifier warns on an atypical `session_close` but honors it, because early
closes legitimately narrow the same field), and an empty calendar produces
Closed. Overnight is the single unreachable session — an e2e that needs it would
have to run between 20:00 and 04:00 ET, which is not CI-viable.

`tests/e2e/chaos_time.rs` records the standing doctrine: system-wide clock jumps
are not injectable in-process, and every decision-relevant clock read in the
session stack is parameterized — `session_and_close_at(client, now)`,
`market_session_at`, `market_session_status_at`, `is_market_open_at` all take
`now`. Exactly three outermost wrappers stamp `Utc::now()` before entering that
stack: `is_market_open`, `market_session`, and `market_session_status`
(`market_hours.rs:65-84`). Boundary logic is today exercised through the
`_at(now)` functions in unit tests; the full-system e2e layer has no equivalent
seam.

The 1952/1953 integration tests cover the scan-layer boundary behavior with
`MockExecutor`, whose session is a plain builder knob. The e2e layer runs the
real `AlpacaBrokerApi` executor against the mock HTTP API, so it inherits the
real classifier and with it the wall-clock anchor.

## Options considered

1. **Test-support clock offset on the Alpaca client (chosen).** A
   `chrono::Duration` offset carried by `AlpacaBrokerApiClient`, present only
   under `#[cfg(feature = "test-support")]`, applied in the three outermost
   wrappers: they stamp `Utc::now() + offset` instead of `Utc::now()`.
   Everything below the wrappers is untouched and already parameterized. A
   `cfg`-off build contains no offset code at all.
2. **A `Clock` trait object on the client.** Same reach, more surface: trait
   definition, injection plumbing, an `Arc<dyn Clock>` on a hot struct, and
   mocking machinery — to control a single stamping site that option 1 covers
   with one field. Rejected as over-engineering for one consumer.
3. **Calendar-span fabrication only.** Already works for Regular/Extended/Closed
   and stays the preferred tool for them, but cannot reach Overnight (the hour
   check reads the clock, not the calendar). Rejected as the sole mechanism;
   retained for what it already does.
4. **Process-external clock faking (libfaketime or similar).** Rejected by the
   `chaos_time.rs` doctrine: monotonic reads (`Instant` caches, tokio intervals)
   and DB-scheduled delays (apalis) would tear from the faked wall clock,
   corrupting exactly the machinery the e2e is supposed to exercise.
5. **Running the e2e suite at fixed times of day.** Not CI-viable, and inverts
   the problem for every non-overnight scenario.

## Decision

Option 1, refined at implementation: the offset rides a new
`AlpacaBrokerApiMode::MockAt { base_url, clock_offset_secs }` variant (same
`cfg(any(test, feature = "mock"))` gate as `Mock`) instead of a field on the ctx
or client. This is strictly better than the proposed field: no struct gains a
member (zero ripple through the ~55 ctx construction sites), the mode enum's
custom Deserialize has no arm for it so no TOML form can ever produce one, and
tying the offset to mock mode makes shifting the clock against sandbox or
production unrepresentable rather than merely unconfigured. The session clock
derives from the mode (`mode.session_clock_now()`), applied at the three
outermost wrappers. The e2e harness gets a helper that computes the offset
landing the effective clock at a chosen ET instant (hour and date), and an
`effective_now()` accessor so tests never hand-mix clocks.

**The two-clock coherence rule** accompanies the seam and is its main hazard,
stated as a contract:

- Session classification runs on the OFFSET clock. Mock calendar fixtures must
  therefore be anchored to the offset clock's date.
- Duration-based checks everywhere else run on the REAL clock against absolute
  timestamps: indicative-quote staleness (`overnight_max_quote_age_secs`
  compares the quote's own timestamp to the caller's `Utc::now()`), reprice
  cadences (`placed_at` vs the scan's `Utc::now()`), apalis scheduling, and —
  the non-obvious one — overnight eligibility staleness.
  `validate_overnight_eligibility`'s callers stamp real `Utc::now()`, and its
  window anchor (`eligibility_sync_window_start(now)`, the most recent 19:45 ET
  at or before `now`) satisfies `window_start(now) <= now` at every real time —
  so a freshly-synced snapshot (the e2e startup sync) always passes regardless
  of the offset session, and stale-sync scenarios simply backdate `synced_at`
  past real-now's window. Eligibility snapshots therefore anchor to the REAL
  clock, like quotes.
- A fixture that needs both (a calendar entry whose `session_close` interacts
  with a reprice cadence) states both anchors explicitly.

The offset deliberately does NOT touch any non-session clock read. Widening it
to a process-global clock would reintroduce option 4's tearing hazard through
the back door.

## Consequences

- Overnight, and every boundary into and out of it (20:00, 04:00, Sunday,
  holiday, half-day, DST nights), becomes drivable from e2e at any real time.
  DST scenarios get date control for free: the offset lands the effective clock
  on the transition night, and the ET conversion below the wrappers does the
  rest.
- The production crate carries a `cfg(test-support)` field on the Alpaca client.
  The feature-flag discipline in `docs/feature-flags.md` applies; a release
  build is bit-identical to today's.
- Fixture authors carry the two-clock coherence burden. The harness helper
  (`clock_offset_secs_to_et`, which pairs the offset with the calendar
  constructors' effective date) is the guardrail; scenario docs state which
  clock each fixture belongs to. An `effective_now()` accessor is added the
  moment a fixture needs offset-clock arithmetic — deliberately not before,
  since its most tempting misuse (stamping quote timestamps) is exactly what the
  coherence rule forbids.
- The seam covers session classification only. If a future test needs to move a
  non-session clock (e.g. to age a quote), it uses the mock's own timestamp
  knobs, never this offset.
