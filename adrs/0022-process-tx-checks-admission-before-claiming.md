# ADR 0022: process-tx checks broker admission before claiming the position

- Status: Accepted
- Date: 2026-09-24

## Context

The `process-tx` operator verb accounts a decoded onchain fill and, when the
position still needs a hedge, places one under the shared submission lock. Under
the schedule aware close flatten policy broker admission can defer a placement
outside the regular session.

`place_offchain_order_at_broker` records the placement intent (`PlaceReserved`,
leaving the order `Pending`) before it runs admission, and `process-tx` claims
the position (`PlaceOffChainOrder`) before calling it. A deferral therefore
arrived after the claim and the `Pending` intent were already durable. Retaining
that intent handed the standing pipeline a `Pending` order whose recovery paths
(`recover_pending_poll_status` and `recover_single_orphaned_order`) replay the
stored shares and reservation terms without rerunning preflight, so changed
cash, equity, hedge floor, or whole share eligibility could make the
resubmission unsafe.

Unwinding the intent after the fact instead needed durable machinery: a failure
kind on the order and position failure events so a deferral would not count as a
hedge failure, a placement provenance so recovery could tell a `process-tx`
intent from a live pipeline one, a recovery retirement for an intent stranded by
a crash mid unwind, a projection column to keep deferral terminals out of trade
history, and matching filters in the reliability and latency projections. All of
it existed only to compensate for writing state before a check that could run
first.

## Decision

`process-tx` asks the order placer for admission (`prepare_placement`) after the
placement preflight and before claiming the position, while holding the same
submission guards as the claim and the placement. Admission is the placer's: the
in bot REST route's placer applies the trading schedule, while the standalone
CLI placer has no admission gate and always admits, so only the REST route can
defer.

- **Deferred:** the verb writes no claim, no `Pending` intent, and no anchor for
  this placement; it settles the accounted fill and reports
  `HedgePlacementDeferred`. With no claim on the position, the standing
  `CheckPositions` pipeline sees the exposure again and hedges it from a fresh
  preflight.
- **Admission error:** nothing is claimed, so the error surfaces with the fill
  left unsettled and a rerun resumes it, the same as a preflight error.
- **Admitted or recovered:** the verb claims the position and places the order.
  The placement runs admission again and adopts an order the broker already
  holds under the same client order id.

If admission changes between the two checks, the placement's own check defers or
errors after the claim. That rare case takes the existing admission failure
path: the still `Pending` order is failed, its id released, the claim cleared,
and the fill settled, so no stale intent is left to replay. A deferral there
still reports `HedgePlacementDeferred`.

## Consequences

- A `process-tx` deferral at the check before the claim leaves no state behind
  for this placement: no claim, no `Pending` intent, no terminal order, and no
  anchor. Besides accounting and settling the fill, the only writes are
  reconciling an earlier claim and releasing an earlier anchor, both of which
  run before admission. Either way the next hedge is sized by a fresh preflight.
- No new persisted fields, projection columns, or migrations are needed, and
  trade history, reliability, and latency projections are untouched.
- Admission runs twice on the admitted path of the REST route, adding one broker
  lookup by client order id and one market session read to each placement while
  the schedule is enabled. The CLI's admission is a no op.
- A deferral that only appears after the claim is recorded as an ordinary failed
  placement, so it counts as a hedge failure in the reliability report. It
  requires the session boundary to fall between two checks made moments apart
  under the same lock.
- A crash between the claim and the broker call leaves a `Pending` intent that
  recovery replays with its stored terms, the same exposure a live pipeline
  `Pending` already carries. Closing that for every placement path is left to a
  separate change.
- Recovery that drives a `Pending` through the broker again holds the cross
  process submission file lock, so even a standalone `process-tx` CLI run
  against a live bot, which the operator procedure forbids, cannot race its
  placement. The lock is defense in depth, not a supported concurrent mode.

## Alternatives considered

- **Retain the Pending intent for the standing pipeline to retry.** Rejected
  because a recovery replay could resend stale shares and reservation terms
  without a fresh preflight.
- **Unwind the intent after a deferral.** Fail the `Pending` order with a
  deferral kind, clear the claim, tag the intent with its provenance, and retire
  a stranded `process-tx` intent in recovery after a broker lookup. Rejected: it
  worked, but it added two durable event fields, a projection migration,
  classification in four recovery paths, and filters in every consumer of failed
  terminals, all to undo state that checking admission first never writes.
- **Enqueue the fill for the standing pipeline instead of placing inline.**
  Smaller still, but it drops the typed synchronous outcome the route and the
  CLI report to the operator.
