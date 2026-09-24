# ADR 0022: A process-tx admission deferral clears the pending claim

- Status: Accepted
- Date: 2026-09-22

## Context

The `process-tx` operator verb accounts a decoded onchain fill and, when the
position still needs a hedge, places one under the shared submission lock. Under
the schedule aware close flatten policy broker admission can defer a placement
outside the regular session. A deferral previously retained the
`OffchainOrder::Pending` intent and left the position claim
(`pending_offchain_order_id`) pointing at it, handing the Pending order to the
standing pipeline to retry.

The live recovery paths that retry a Pending order,
`recover_pending_poll_status` at `src/trading/offchain/hedge.rs` and
`recover_single_orphaned_order` at `src/conductor.rs`, drive the stored order
back through the broker blind. They replay the durable shares and reservation
terms without rerunning preflight, so changed cash, equity, hedge floor, or
whole share eligibility could make the resubmission unsafe. Those paths also
cannot tell a never sent deferred intent apart from a crash orphan whose broker
outcome was lost, because both rest in the same `Pending` state keyed off the
position claim.

Clearing the claim is not atomic with recording the intent. `process-tx` writes
the position claim and then records the placement intent (`PlaceReserved`,
leaving the order `Pending`) before broker admission runs, so a crash between an
admission deferral and the retirement below leaves exactly the durable state the
deferral was meant to remove: a claimed position pointing at a `Pending` order
carrying stored shares and, for a buy, a stored buying power reservation. Both
recovery paths would then replay that intent with no fresh preflight, which is
the replay this decision set out to avoid. The `Pending` state alone cannot tell
the two apart, because a live pipeline `Pending` is a legitimate retained intent
and a `process-tx` `Pending` is not.

## Decision

On a `process-tx` admission deferral, do not retain a Pending intent. Fail the
still Pending `OffchainOrder` and clear the position claim to the retry eligible
state, exactly as the failed placement path does. The terminal records why it
was retired: the `MarkPlacementFailed` command and the `Failed` event carry an
`OffchainOrderFailureKind`, and a deferral persists `Deferral` while every other
retirement persists `Failure`. A deferral and an admission failure both return
before the broker call, so the retired order id is released instead of being
kept as the idempotency anchor; only a failure whose broker call did run, such
as backpressure, preserves it. The fill is settled and the deferral is reported.
The standing `CheckPositions` pipeline then detects the unhedged exposure again
and preflights a fresh hedge from scratch.

Clearing the claim removes the only pointer the recovery paths scan, so the
abandoned Pending order is never driven back through the broker. Every retry
runs a fresh preflight against current cash, equity, hedge floor, and whole
share eligibility rather than replaying stale terms.

A placement intent therefore records its provenance. `PlacementProvenance` is a
durable field on the `PlaceReserved` command, the `Placed` event, and the
`Pending` entity; it defaults to `LivePipeline`, so legacy events and every live
pipeline placement are unchanged, and `process-tx` sets `ProcessTx`. Both
recovery paths, `recover_single_orphaned_order` and
`recover_pending_poll_status`, reconcile a `ProcessTx` `Pending` against the
broker by `client_order_id` before acting on it. An order under that key proves
the placement did reach the broker, so the ordinary replay adopts it. A lookup
that finds nothing retires it: the order fails with the `Deferral` kind and the
claim is cleared. A current not found answer is not proof the broker never
recorded the order, because the POST can succeed before its outcome is persisted
and the lookup endpoint does not guarantee retention, so the id is kept as the
idempotency anchor. The next hedge for the symbol then goes through the standard
anchor reconciliation instead of minting a fresh client order id the broker
could not dedupe against the original. A lookup that cannot be answered leaves
the intent claimed for the next sweep, since neither replaying nor retiring is
safe without an answer. An executor with no order lookup keeps the ordinary
replay, the same gate `reconcile_failed_anchor` uses. Only `Pending` is the
crash window: an order that reached `Submitted` or `Accepted` is driven by the
poll path instead.

## Consequences

- A `process-tx` deferral never leaves stale shares or reservation terms for a
  recovery path to replay: the claim is cleared on the deferral itself, and an
  intent stranded by a crash in the admission window is retired once a broker
  lookup finds nothing under the client order id. The next hedge is always sized
  by a fresh preflight.
- An order id retired on the deferral itself, whose broker call provably never
  ran, is not kept as the idempotency anchor. Releasing it stops
  `CheckPositions` from skipping the position and scheduling an anchor
  reconciliation lookup at the broker for an order that was never created.
- An intent retired by recovery keeps its id as the anchor, since the lookup
  cannot prove the broker never saw it. The cost is one anchor reconciliation
  cycle before the next fresh hedge; releasing it instead risks a second live
  order.
- The durable additions are small and default safe: the
  `OffchainOrderFailureKind` field on the `Failed` event and the
  `PlacementProvenance` field on the `Placed` event and `PlaceReserved` command.
  Older payloads omit both and read back as `Failure` and `LivePipeline`. No
  database migration is needed.
- The `process-tx` route reports the deferral, while the standing pipeline owns
  the retry.
- A deferral is persisted as a deferral kind terminal, so the reliability
  projection does not count it as a hedge failure and no
  `lifecycle_failure_event` row is written for it. Genuine failures, including
  events persisted before the discriminator existed, still count: the event
  field defaults to `Failure` when absent.
- The retained intent handling added earlier for the `process-tx` case is
  removed. The Pending classification observed before placement now covers only
  a hedge the live pipeline itself deferred and is holding for its own retry,
  which `process-tx` settles and reports without placing a second hedge over it.
- A recovery replay of a live pipeline `Pending` still resends stored shares and
  reservation terms without rerunning preflight. That exposure is broader than
  the `process-tx` window and is left to a separate change.
- The `OffchainOrder` schema version is bumped so projections carry the new
  provenance field.

## Alternatives considered

- **Retain the Pending intent for the standing pipeline to retry (Option 1).**
  Rejected because a recovery replay could resend stale shares and reservation
  terms without a fresh preflight, and because the recovery paths cannot
  distinguish a never sent deferral from a crash orphan whose broker outcome was
  lost.
