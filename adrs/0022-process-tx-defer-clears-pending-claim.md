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

## Decision

On a `process-tx` admission deferral, do not retain a Pending intent. Fail the
still Pending `OffchainOrder` and clear the position claim to the retry eligible
state, exactly as the failed placement path does, preserving the idempotency
anchor. The fill is settled and the deferral is reported. The standing
`CheckPositions` pipeline then detects the unhedged exposure again and
preflights a fresh hedge from scratch.

Clearing the claim removes the only pointer the recovery paths scan, so the
abandoned Pending order is never driven back through the broker. Every retry
runs a fresh preflight against current cash, equity, hedge floor, and whole
share eligibility rather than replaying stale terms.

## Consequences

- No stale shares or reservation terms are ever replayed by a recovery path. The
  next hedge is always sized by a fresh preflight.
- No schema change is needed. The behavior lives entirely in the placement flow
  and the aggregate commands it already uses.
- The `process-tx` route reports the deferral, while the standing pipeline owns
  the retry.
- The retained intent handling added earlier for the `process-tx` case is
  removed. The Pending classification observed before placement now covers only
  a hedge the live pipeline itself deferred and is holding for its own retry,
  which `process-tx` settles and reports without placing a second hedge over it.

## Alternatives considered

- **Retain the Pending intent for the standing pipeline to retry (Option 1).**
  Rejected because a recovery replay could resend stale shares and reservation
  terms without a fresh preflight, and because the recovery paths cannot
  distinguish a never sent deferral from a crash orphan whose broker outcome was
  lost.
