# ADR 0014: Recover stuck Pending placements at runtime, and serialize broker-placement attempts

- Status: Accepted
- Date: 2026-06-23

## Context

ADR 0009 lifted the broker call into the durable
`place_offchain_order_at_broker` path and asserted that the periodic
`CheckPositions` job is the runtime safety net for a placement that crashes
between the broker accepting an order and the `MarkAccepted` commit. That safety
net does not actually cover this case. `Position::is_ready_for_execution`
(`src/position.rs:620`) short-circuits to `Ok(None)` whenever
`pending_offchain_order_id.is_some()`, so `CheckPositions` _skips_ exactly the
positions whose placement is stuck. Two real defects follow from this.

### Defect 1: a stuck `Pending` placement is only recovered at the next restart

Sequence: the trade-accounting reactor (`process_queued_trade`) acknowledges the
fill, then `Position::PlaceOffChainOrder` claims
`pending_offchain_order_id = A`, then `place_offchain_order_at_broker` runs
`Place` (order A -> `Pending`) and the broker accepts -- but the process dies
(or the DB write fails) before `MarkAccepted` commits. Order A is now `Pending`
with a live broker order; the position still holds
`pending_offchain_order_id = A`. On the apalis retry, `process_queued_trade`
sees the trade already acknowledged and returns `Ok(None)` (ADR 0005) before
reaching placement. `CheckPositions` skips the position (pending set).
`recover_submitted_offchain_orders` skips `Pending`. The hedge job's
`recover_pending_poll_status` only runs when a `PlaceHedge` job is enqueued and
rejected with `PendingExecution`, which `CheckPositions` will never enqueue for
this position. The only thing that re-drives order A is
`recover_orphaned_pending_offchain_orders`, which runs **once, at startup**
(`conductor.rs:600`). The hedge is unreconciled and the exposure unhedged until
an operator restarts the bot.

### Defect 2: a stale `MarkFailed` can strand a live order as `Failed`

The original placement runs under the global `counter_trade_submission_lock`
(`conductor.rs:165`, a single `Arc<Mutex<()>>`), but the hedge job
(`PlaceHedge::perform`) and its `recover_pending_poll_status` re-drive do
**not** take that lock. So the original placement of order A and a hedge-path
re-drive of the same order A can call `place_offchain_order_at_broker(A)`
concurrently, both observing `Pending`. If the erroring attempt commits
`MarkFailed` first, order A goes `Failed`; the succeeding attempt's
`MarkAccepted` then no-ops on `Failed` (the idempotent arm), **dropping the
`executor_order_id` of a live broker order.** A retry sees the non-`Pending`
state, skips the broker, and returns `Failed` forever. The existing `MarkFailed`
TOCTOU guard only covers one direction.

## Decision

1. **Serialize every broker-placement attempt for a symbol.** The hedge job and
   its `recover_pending_poll_status` re-drive acquire the same
   `counter_trade_submission_lock` the trade-accounting path already holds,
   around the `PlaceOffChainOrder` claim through the
   `place_offchain_order_at_broker` call. With all placement attempts
   serialized, a second attempt on an order that another attempt already drove
   finds it past `Pending` and skips the broker (the existing early-return), so
   the `MarkFailed`/`MarkAccepted` race cannot occur. This is a locking change
   only -- the `OffchainOrder` state machine is unchanged.

2. **Run stuck-`Pending` recovery at runtime, not only at startup.** Invoke the
   existing `recover_orphaned_pending_offchain_orders` sweep (which scans
   positions holding a `pending_offchain_order_id` and re-drives a `Pending`
   order through `place_offchain_order_at_broker`) on the `CheckPositions`
   cadence, so a placement stuck between broker acceptance and the outcome
   commit is reconciled within one check interval instead of at the next
   restart. The broker dedupes the re-drive on `client_order_id` and adopts the
   in-flight order; `Place` is a no-op on the existing aggregate. The re-drive
   runs under the same placement lock (decision 1).

This amends ADR 0005 (the placement step's recovery is no longer startup-only)
and corrects the runtime-safety-net argument in ADR 0009's Context.

## Consequences

### Positive

- A placement stuck mid-flight is reconciled within one `CheckPositions`
  interval, bounding unhedged exposure to that window instead of "until
  restart."
- The `MarkFailed`/`MarkAccepted` race is eliminated at the source: no two
  attempts drive the same order's broker call concurrently, so a live order can
  never be stranded as `Failed`.

### Negative / costs

- All broker placements now serialize on one global lock. Placements are
  broker-I/O-bound and not high-frequency, so the throughput cost is small, but
  it is a real global serialization point.
- The periodic recovery adds a per-interval scan of pending-claimed positions
  and an idempotent broker re-drive for any found `Pending` -- bounded work, but
  non-zero, and one extra broker round-trip per stuck order per interval until
  it resolves.

### Neutral

- `recover_orphaned_pending_offchain_orders` is reused, not duplicated; only its
  invocation cadence changes.
- The over-fill decision in ADR 0009 stands unchanged; only its recovery Context
  is corrected.

## Alternatives considered

- **Broker-acceptance-wins (resurrect `Failed` -> `Submitted` on
  `MarkAccepted`).** Fixes the order aggregate but leaves a position-level
  inconsistency: the erroring attempt's `FailOffChainOrder` already cleared the
  position claim, so a resurrected `Submitted` order would be polled to a fill
  the position no longer expects. It also adds a new `Failed -> Submitted` state
  edge every downstream consumer of `Failed` must tolerate. Serialization avoids
  the race entirely without either cost.
- **Mirror the `MarkFailed` TOCTOU guard for `MarkAccepted`.** Stops the dropped
  acceptance but still leaves the live order stranded as `Failed`. Strictly
  worse than preventing the race.
- **Accounting-path `PendingExecution` re-drive only.** The accounting retry
  short-circuits at "acknowledged" before reaching placement, so this never
  fires for the stranded-`Pending` case. Insufficient on its own.
- **Make `CheckPositions` ignore the pending guard.** Would double-place orders
  that are legitimately in-flight and not stuck. Targeting only `Pending` orders
  via the existing recovery sweep is the precise fix.

## Follow-ups

- Done (implemented in this PR): `counter_trade_submission_lock` is taken in
  `PlaceHedge::perform` (and the `recover_pending_poll_status` re-drive) around
  claim-through-placement; covered by a test that `PlaceHedge::perform` blocks
  while the placement lock is held, so the original placement and a concurrent
  re-drive of the same order cannot both reach the broker.
- Done (implemented in this PR): `recover_orphaned_pending_offchain_orders` is
  invoked on the `CheckPositions` cadence; covered by a test that a
  position-claimed `Pending` order is re-driven at runtime, not only at startup.
- Done (implemented in this PR): ADR 0009's status header notes its
  runtime-recovery Context is corrected by this ADR.
