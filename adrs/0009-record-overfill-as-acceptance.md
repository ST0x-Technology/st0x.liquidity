# ADR 0009: Record a broker over-fill as an acceptance, not a failure

- Status: Accepted (runtime-recovery Context corrected by ADR 0014)
- Date: 2026-06-23

## Context

The broker `place_market_order` call lives in the durable
`place_offchain_order_at_broker` path (not the pure `OffchainOrder::Place`
handler), which decides the order's outcome from the broker
`OrderPlacementResult`:

- `placed_shares <= requested` -> `MarkAccepted` (order becomes `Submitted`,
  carries the `executor_order_id`, gets a `PollOrderStatus` job).
- broker error -> `MarkFailed`.
- `placed_shares > requested` (over-fill) -> currently `MarkFailed`.

Three durability concerns in the placement path are worth recording. Two are
**not** defects:

- **Acknowledge-before-placement window.** `process_queued_trade` marks the
  onchain trade acknowledged before placing the hedge, and the step-grained
  dedupe (ADR 0005) skips any acknowledged trade on redelivery. This does not
  lose the hedge: `AcknowledgeOnChainFill` moves the `Position`'s net exposure,
  and the periodic `CheckPositions` job (`src/position_check.rs`) re-scans every
  position by net exposure -- independent of `pending_offchain_order_id` -- and
  enqueues a `PlaceHedge`. ADR 0005 deliberately relies on this ("a retry ...
  returns `Ok(None)` even when hedge placement is still impossible"). Reordering
  acknowledge and placement is the inversion ADR 0005 rejected as "strictly
  worse for financial integrity."
- **Trade-accounting path lacks the `PendingExecution` recovery the hedge job
  has.** The trade-accounting path returns `Ok(None)` on a `PendingExecution`
  rejection rather than re-driving. Same safety net: the next `CheckPositions`
  scan re-enqueues a `PlaceHedge`, which carries a `Pending` re-drive. No fill
  is stranded.

The third is a real, if rare, financial-integrity defect. Treating an over-fill
as `MarkFailed`:

1. discards the `executor_order_id`, so the live broker order can never be
   polled, reconciled, or cancelled;
2. clears the position claim and stashes the order id as the idempotency anchor,
   so the next `CheckPositions` scan re-drives placement under the same
   `client_order_id`, the broker dedupes and returns the same over-filled order,
   and the `> requested` branch fires again -- an unbounded re-drive loop around
   a live, unpolled broker position.

An over-fill is shares the broker actually bought; recording it as a failure
masks real financial state, which AGENTS.md's financial-integrity rule forbids
("NEVER silently mask failures on financial values").

## Decision

Treat a broker over-fill (`placed_shares > requested`) as an **acceptance**, not
a failure:

1. The `placed_shares > requested` branch emits
   `MarkAccepted { placed_shares,
   .. }` carrying the broker-accepted
   quantity, exactly like the normal acceptance branch. The order becomes
   `Submitted`, carries its `executor_order_id`, and gets a `PollOrderStatus`
   job.
2. The order's `shares` therefore reflect the actual broker-working quantity.
   The `Accepted` event already records `placed_shares` (which may exceed the
   request); `OffchainOrder::Submitted.shares` is the broker-accepted amount,
   which may exceed the requested quantity.
3. The excess over the requested hedge is real exposure in the opposite
   direction; the `Position` aggregate and the periodic `CheckPositions` scan
   reconcile it like any other net exposure -- they hedge the residual on a
   later scan.
4. Log the over-fill at `warn` so an operator sees the anomalous broker
   behavior.
5. Remove the now-unused `OffchainOrderError::PlacedExceedsRequested` variant
   (it is constructed nowhere once the over-fill no longer fails).

## Consequences

### Positive

- No live broker order is ever recorded as `Failed`: every accepted order --
  including an over-fill -- carries its `executor_order_id` and is polled to a
  terminal state.
- The unbounded re-drive loop is eliminated: an over-fill reaches `Submitted`
  and is polled, never re-placed.
- Financial state matches reality: the system accounts for the shares the broker
  actually bought.

### Negative / costs

- An over-fill momentarily over-hedges the position by the excess; the residual
  opposite exposure is corrected on a later `CheckPositions` scan rather than
  instantly. For a fractional-share market-order over-fill this is negligible,
  but a pathological large over-fill would book a correspondingly large
  transient over-hedge before correction.
- We drop the explicit "reject over-fill" guard; a broker that systematically
  over-fills has its excess absorbed (with a `warn` log) rather than surfaced as
  a job failure.

### Neutral

- `PlacedExceedsRequested` is removed; the over-fill quantities live in the
  `warn` log and the `Accepted`/`Submitted` `placed_shares`.
- No change to the two non-defects (acknowledge ordering, trade-accounting
  recovery); they remain as ADR 0005 designed them.

## Alternatives considered

- **Keep `MarkFailed` (status quo).** Masks real shares as a failure, strands a
  live unpolled order, and loops. Rejected on financial-integrity grounds.
- **Cancel the broker order, then `MarkFailed`.** Restores "the hedge did not
  happen," but needs a broker cancel call that can fail or race the fill (the
  order may already be filling), reintroducing the live-order problem -- more
  moving parts for a near-impossible broker behavior.
- **`MarkAccepted` plus an alert/halt above a configurable excess threshold.**
  Safer against a pathological over-fill, but adds config and a halt path for a
  case never observed; can be layered on later (see follow-ups).

## Follow-ups

- Done (implemented in this PR): the `MarkAccepted`-on-over-fill change in
  `place_offchain_order_at_broker`; `PlacedExceedsRequested` dropped.
- Done (implemented in this PR): `place_at_broker_with_overfill_marks_failed`
  replaced by `place_at_broker_with_overfill_records_acceptance`, asserting the
  over-fill reaches `Submitted` with `placed_shares` recorded and a poll
  enqueued.
- (Deferred) a configurable over-fill excess threshold that halts/alerts instead
  of absorbing, if a real over-fill is ever observed.
