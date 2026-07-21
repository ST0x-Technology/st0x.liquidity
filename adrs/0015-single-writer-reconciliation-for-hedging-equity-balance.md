# ADR 0015: Single-writer reconciliation for the Hedging equity balance

- Status: Proposed
- Date: 2026-07-20

## Context

Two independent writers set the Hedging (offchain) available equity balance in
`InventoryView`, and neither accounts for the other:

1. **Snapshots.** `poll_offchain` (`src/inventory/polling.rs:507`) reads the
   broker every ~60s; `apply_equity_snapshot` (`src/inventory/view.rs`) **sets**
   the balance to the polled number.
2. **Fill deltas.** `RebalancingService::react`
   (`src/rebalancing/trigger/mod.rs`) turns `PositionEvent::OffChainOrderFilled`
   into an **add/remove** of the filled shares. `InventoryView` never sees
   `PositionEvent` directly.

Alpaca bakes a fill into `GET /positions` the instant it executes, which is
before we observe the fill. A snapshot landing in that window already contains
the fill, and the delta is then applied on top — counting the same shares twice.

The bug has two variants. When the fill exceeds the remaining balance the
subtraction underflows, `and_then` discards both legs, and the balance is left
at the snapshot value — correct, by accident of the error path. That is the
_loud_ variant, the _MSTR incident of 2026-05-26_. When the fill is smaller the
subtraction **succeeds** and the balance is silently short by one fill until the
next poll. That is the _silent_ variant that actually drives needless
rebalancing transfers, and it is the more common one.

Existing mechanisms this fix builds on:

- `equity_snapshot_would_apply` already guards snapshots on a per-symbol
  watermark, inflight transfers, and `last_rebalancing` — precedent for the
  guard shape below. It is called per symbol inside a `try_fold`, so skipping
  one symbol leaves the rest of the snapshot unaffected, and the watermark
  advances only for symbols that actually applied. A skipped symbol can heal on
  a later poll — but only if that poll _emits_: the `InventorySnapshot`
  aggregate dedupes unchanged values, which the forget-on-clear mechanism below
  exists to defeat.
- Commit
  [`1937c4b0`](https://github.com/ST0x-Technology/st0x.liquidity/commit/1937c4b0)
  ([PR #690](https://github.com/ST0x-Technology/st0x.liquidity/pull/690)) made
  the pending-order gate clear on any terminal offchain event even when the
  inventory update fails. That is the deadlock safety net and is preserved
  unchanged.
- `InventoryView` has no table of its own: it is derived state, rebuilt at every
  startup by replaying the persisted `InventorySnapshot` aggregate through
  `apply_snapshot_event` (`hydrate_inventory_from_snapshot`,
  `conductor.rs:631`). New view fields therefore need no migration and no
  event-schema change.
- `RebalancingService`'s pending-order state was likewise runtime-only — an
  `Arc<RwLock<HashSet<Symbol>>>` rebuilt from the `Position` projection at
  startup, never persisted. Moving it into the view moves in-memory state to
  in-memory state, so it carries no schema or DTO impact either.

## Decision

**Give the Hedging equity balance a single writer at any instant**, by extending
`equity_snapshot_would_apply` with two guards that mirror the existing
`last_rebalancing` guard. For `Venue::Hedging` only, skip a symbol when:

1. **A hedge order is open for it.** The fill delta owns the balance until the
   order reaches a terminal state.
2. **The snapshot predates the last applied fill**
   (`fetched_at < last_offchain_fill_applied_at`), covering the reverse
   ordering.

Neither guard compares clocks that can disagree: guard 1 involves no timestamp
at all, and guard 2 compares two readings of **this host's clock**. That is the
property alternative D (the broker-timestamp comparison) lacks.

The `pending_offchain_order_symbols` field **moves** from `RebalancingService`
into `InventoryView` — not mirrored: two copies under two locks, updated by hand
from the same events, is the duplicated-state pattern behind the incident fixed
in commit
[`cd60137e`](https://github.com/ST0x-Technology/st0x.liquidity/commit/cd60137e)
([PR #1013](https://github.com/ST0x-Technology/st0x.liquidity/pull/1013)) — a
missed cleanup on one of three copies of the USDC in-flight fact wedged
rebalancing until restart. The view is the right owner because the set is now a
_balance-integrity_ input, not just a trigger gate: it decides whether a
snapshot may write, alongside the watermarks, inflight, and `last_rebalancing`
state the view already owns for exactly that purpose — and it inherits the
view's lock, so the fill path can apply the delta and release the block in one
critical section, which no cross-struct copy could guarantee.

- New `InventoryView` fields: `pending_offchain_order_symbols`,
  `last_offchain_fill_applied_at`, with mark/clear/has/set accessors.
- `RebalancingService::pending_offchain_order_symbols` is **deleted**; the
  trigger's `has_pending_offchain_order` reads the view, and startup recovery
  seeds the view instead.
- Snapshot-error recovery (`on_snapshot_recovery`) resets the view but
  **preserves the guard state** via `reset_preserving_offchain_order_state`: the
  state is fed by `Position` events and nothing re-seeds it after startup, so a
  plain `default()` reset would silently clear every open-hedge block and
  re-open the race. Preservation (not re-derivation) because the applied-fill
  times are local-clock readings no projection stores.
- **Forget-on-clear** makes skipped snapshots retryable. The aggregate dedupes
  unchanged positions, so a snapshot skipped during an open order would never
  re-emit on its own — leaving the view stale until the position changed or a
  restart, and silently breaking the "next snapshot heals" assumption both PR
  #690 and the guards lean on. On gate clear the trigger sends
  `ForgetOffchainEquity { symbol }`; the aggregate marks the symbol undelivered
  (in a `offchain_equity_forgotten` set — the cached map stays intact so startup
  hydration still seeds last-known balances) and the next poll re-emits broker
  truth with a fresh local stamp, which passes guard 2 and heals within one poll
  of the clear. The skipped value itself is never replayed — it is ambiguous
  mid-order data; only fresh reads are ever applied.
- On a fill, the block is released inside the **same write-lock critical
  section** that applied the delta, so no snapshot can interleave. The recorded
  time is `Utc::now()`, not the event's `broker_timestamp` — it is compared
  against `fetched_at`, which this host also stamps (mixing them would
  reintroduce alternative D's cross-clock defect). It marks when the fill was
  _applied_, slightly after the true fill instant, so guard 2 rejects marginally
  more snapshots than necessary — the safe direction.
- **`fetched_at` is captured before the broker read**, in the poller, and
  carried through the command (matching the pre-existing `InflightEquity`
  pattern) — not stamped at command-handling time. A command-time stamp
  postdates the read, so a pre-fill read could be stamped after the fill was
  applied and slip past guard 2. A before-read stamp lower-bounds the broker's
  as-of time, so a pre-fill read always stamps before the fill's execution and
  guard 2 catches it structurally. Cost: a post-fill read whose request started
  pre-execution is rejected and retries next poll — the safe direction again.

Walking the incident through: the order is placed, marking MSTR pending; the
16:08:34.711 poll is skipped for MSTR and its watermark does not advance; the
fill subtracts 5.143 from the pre-fill baseline giving 4.698; the next poll
confirms it.

**Invariant relied on (pre-existing): At most one offchain order is open per
symbol** at any point in time — enforced by `Position`'s singular
`pending_offchain_order_id` (`position.rs:35`), the projection ignoring a second
`OffChainOrderPlaced` while one is pending (`position.rs:249`),
`validate_pending_execution` (`position.rs:814`), and ADR 0014's serialized
placement. The gate this ADR replaces already depended on it identically. If
serialized placement is ever relaxed, a boolean per symbol becomes wrong and
this must become a count or a set of order ids.

## Tradeoffs & Consequences

**Accepted downside:** while a hedge order is open, the symbol's offchain
balance is frozen relative to external truth — a manual trade or corporate
action on that position is invisible until the order reaches a terminal state,
and a leaked pending flag starves that symbol's snapshots for as long as it is
held.

Judged acceptable because it is bounded by the mechanisms that already bound the
gate (PR #690's guaranteed clear, plus startup recovery), and because it is not
a new class of exposure: the existing `has_inflight()` arm of the same function
already starves snapshots this way during rebalancing transfers.

**Blocked snapshots defer healing, not freshness.** Fills keep the mirror
current per-trade; a blocked snapshot only defers what deltas cannot see —
external moves, corporate actions, and recovery from a _failed_ fill
application. The gate is held ~2s per market hedge against 60s polls, so
sustained starvation needs long-resting orders or a leaked flag. The residue:
failed fill applications grow with volume, and each waits for an unblocked
snapshot — what open question 1's staleness bound would cap.

**It extends the lifetime of pre-existing drift.** The guard rejects the
symbol's whole snapshot, not just the contested fill, so unrelated drift loses
the poll that would have healed it — and a dropped fill event has no catch-up
replay to correct it. Healing is layered: the next unblocked poll re-bases plain
drift; a leaked flag clears on the next order's terminal event (the clear is
keyed by symbol, not order id); restart is the backstop — recovery rebuilds the
pending set from the events table and hydration re-bases.

**Starvation is currently invisible in production.** Both guards log their skip
at `debug!`, so a symbol being persistently blocked produces no signal at
production log levels. If unbounded starvation is the accepted risk, it should
at least be observable — a `warn!` after N consecutive skips for the same symbol
would surface it. Not implemented here.

**Scope.** `src/inventory/view.rs` and `src/rebalancing/trigger/mod.rs`, plus
tests. No migration, no event-schema change, no persisted state.

**New startup ordering dependency.** Hydration replays snapshot events _through_
`apply_equity_snapshot`, so these guards now run during it. That is safe only
because `hydrate_inventory_from_snapshot` (`conductor.rs:631`) runs before
`recover_pending_offchain_order_symbols` (`conductor.rs:1514`), leaving the
pending set empty and both guards inert while the view warms up. Reversing the
two would skip hydration for every symbol with an open order, starting it with
an uninitialized offchain balance. Nothing currently enforces the order.

## Alternatives considered

**A. Make the fill path absolute: on Filled, re-fetch positions and set the
balance.** The strongest challenger: every write becomes a broker read stamped
on the local clock, last-write-wins, no guards at all. Rejected on cost: broker
I/O and a retry policy inside a reactor that today has zero executor dependency
(broker reads deliberately live in the poller), and it still needs the watermark
machinery for read-vs-read ordering. Revisit if guard-1 starvation becomes
chronic — nothing built here blocks switching.

**B. Model the open order as an inflight reservation.** The closest call —
inflight already blocks snapshots. Rejected: reservations need exact quantities,
and neither a buy's cost nor an over-fill (ADR 0009) is knowable upfront. The
chosen gate is quantity-blind, so neither can hurt it.

**C. Consume Alpaca's account-activities stream.** The only option that
dissolves the ambiguity at source, and the right long-term successor. Rejected
as a new integration with its own gap/replay semantics, beyond this ticket's
scope; the chosen guards become dead code if it lands.

**D. Compare `broker_timestamp` against the snapshot watermark.** ~20 lines, and
one of the three directions PR #690 names (the chosen approach combines its
other two). Rejected: it compares **two unsynchronized clocks** — Alpaca's vs
ours — and NTP skew is the size of the ~100ms window being closed, so it would
be wrong about as often as right, invisibly.

## Open questions

To settle within the scope of this change.

1. **Should guard 1 carry a staleness bound?** Today a pending flag that never
   clears blocks that symbol's offchain snapshots indefinitely. A bound would
   let broker truth back in after some threshold, trading starvation risk for a
   reintroduced double-apply window. Deliberately left unbounded for now; needs
   a decision rather than a default. See "blocked snapshots defer healing" above
   — the cases a bound would cap are long-resting orders, a leaked flag, and a
   failed fill application waiting through blocked polls.

## Follow-up work

Out of scope here; each is its own change.

1. **The cash leg is unguarded.** The fill applies a mirrored USDC delta at the
   Hedging venue, and the `OffchainUsd` snapshot comes from the _same_
   `get_inventory()` call, so it is double-counted in exactly the same window.
   Only the equity leg is guarded here. The cash balance is venue-level rather
   than per-symbol, so it needs a different mechanism.
2. **Parallel state elsewhere.** `InventoryView` and `RebalancingService` hold
   several other overlapping representations: `active_mints` vs `mint_tracking`,
   `active_redemptions` vs `redemption_tracking`, and `active_usdc_rebalance` vs
   `usdc_tracking` vs `usdc_in_progress`. The USDC triple already caused a
   production incident — terminal cleanup skipped one of the three and latched
   rebalancing off — fixed in commit
   [`cd60137e`](https://github.com/ST0x-Technology/st0x.liquidity/commit/cd60137e)
   ([PR #1013](https://github.com/ST0x-Technology/st0x.liquidity/pull/1013)).
   Consolidating them is out of scope here but is the same class of defect.
