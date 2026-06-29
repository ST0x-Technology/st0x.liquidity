# ADR 0010: Bounded pending-acknowledgement set for cross-process exactly-once

## Status

Accepted. Supersedes in part [ADR 0005](0005-exactly-once-fill-accounting.md)
(the single-slot `last_acknowledged_trade_id` dedup).

## Context

ADR 0005 made fill accounting exactly-once by tracking acknowledgement on the
`OnChainTrade` aggregate and, as defense in depth for the crash window between
the `Position` write and the `OnChainTrade` marker, having `Position` reject a
`trade_id` it has already applied. State stayed bounded by remembering only the
single most-recently applied `trade_id` (`last_acknowledged_trade_id`).

That single slot is correct **only under serialized, in-order processing** --
the property ADR 0005 relied on: the bot's accounting worker is `concurrency(1)`
plus a per-symbol in-process lock, and re-deliveries drain in enqueue order, so
the only trade that can be re-driven against an unmarked acknowledgement is the
most recent one.

The `process-tx` recovery CLI breaks that assumption. It runs in a **separate
process** from the bot, so the in-process per-symbol lock cannot serialize the
two. The double-count window:

1. A process witnesses and acknowledges fill A (the `Position` write lands, the
   single slot now holds A), then crashes before writing the `OnChainTrade`
   marker. A is witnessed-but-unacknowledged; the position already counted it.
2. A newer fill B is acknowledged, advancing the single slot to B. The slot no
   longer remembers A.
3. A is re-driven (the bot rescans, an orphaned accounting job is requeued, or a
   retry fires). The resume path re-runs the acknowledge; the single slot holds
   B, not A, so the guard does not fire and A is counted a **second time**.

The same window exists for any concurrency greater than one, not just the CLI. A
fixed-capacity ring or a scalar high-water-mark cannot close it: an orphaned
re-drive can be delayed across arbitrarily many intervening distinct fills, and
`trade_id` has no processing-order monotonicity across the witness/acknowledge
gap, so no fixed `N` is provably correct. ADR 0005 already rejected a
`Position`-side set of **all** `trade_id`s because it grows unboundedly for the
life of the symbol.

## Decision

`Position` keeps the single slot **and** gains a bounded, event-sourced set of
applied-but-not-yet-settled `trade_id`s, `pending_acknowledged_trade_ids`. The
acknowledge dedup rejects a fill whose `trade_id` either equals the slot **or**
is a member of the set.

1. **Dedicated insert event.** `AcknowledgeOnChainFill` emits, in one atomic
   batch, the existing `OnChainOrderFilled` (the position move, which still
   writes the slot, unchanged) **and** a new `OnChainFillApplied { trade_id }`
   whose `evolve` inserts `trade_id` into the set. The insert is a dedicated
   event -- never folded into `OnChainOrderFilled` -- so that pre-ADR-0010
   history, which contains only `OnChainOrderFilled`, rebuilds the set **empty**
   on a full replay (see "Why the dedicated event").
2. **Prune step.** A new `SettleOnChainFill { trade_id }` command emits
   `OnChainFillSettled { trade_id }` (whose `evolve` removes `trade_id` from the
   set) when the trade is a member, and emits no events otherwise (an
   idempotent, optimistic-concurrency-safe no-op). The accounting pipeline
   issues it as a fifth step, after the marker is durable:
   `witness -> enrich -> acknowledge -> mark -> settle`. The settle runs on
   every post-mark path, including the bot's resume "already acknowledged ->
   skip" branch and the CLI's "already accounted -> refuse" branch, so a leaked
   entry self-heals on the next interaction with that fill.

## Why the dedicated event

`Position` uses the default `COMPACTION_POLICY = Retain`, so no events are ever
deleted. The `SCHEMA_VERSION` bump (3 -> 4) clears `Position`'s snapshot, and
the store then rebuilds the aggregate by replaying every event from the start.
If the set-insert lived on `OnChainOrderFilled`, replaying a long-lived symbol's
history would insert **every historical** `trade_id` into the set, with no
matching `OnChainFillSettled` events to remove them (the event did not exist
pre-ADR-0010) -- re-creating the unbounded set ADR 0005 rejected, on the first
reload after deploy. Because the insert lives on the dedicated
`OnChainFillApplied` event, pre-ADR-0010 history contains zero inserts and
rebuilds the set empty; post-ADR-0010 fills replay as `OnChainFillApplied`
(insert) / `OnChainFillSettled` (remove) pairs that converge to just the
genuinely in-flight tail.

## Why the slot is retained

A fill applied under pre-ADR-0010 code but left unmarked when the deploy
restarts the process has no `OnChainFillApplied` event in its history, so the
rebuilt set cannot guard it. The retained single slot -- written by the
unchanged `OnChainOrderFilled` evolve -- still equals that `trade_id` and
rejects its re-drive, bridging the upgrade window with **zero migration**.
Seeding the set at migration from witnessed-but-unacknowledged `OnChainTrade`s
is unsound: the marker cannot distinguish a witness->acknowledge crash (the
position never applied the fill -- seeding would make a real fill un-applyable,
an under-count) from an acknowledge->mark crash (the position did apply it).
Once all in-flight-at-upgrade fills have drained, the slot is
redundant-but-harmless (a distinct fill never equals it) and may be removed by a
deliberate follow-up.

## Alternatives considered

- **Reactor/projection driving `Position` from `OnChainTrade::Acknowledged`.**
  Would give a single authoritative write, but reactors in this codebase are
  live-only with no replay cursor; making it exactly-once needs a persisted
  cursor plus startup replay (the cross-aggregate atomicity the framework
  deliberately lacks) and still needs a `Position`-side dedup for the cursor's
  at-least-once redelivery. Most complex, and fights the framework.
- **Fixed-capacity ring / scalar high-water-mark.** Bounded and replay-trivial,
  but not provably correct (see Context).
- **`Position`-side set of all `trade_id`s.** The unbounded growth ADR 0005
  rejected.

## Consequences

- New `Position` events (`OnChainFillApplied`, `OnChainFillSettled`) and command
  (`SettleOnChainFill`); `OnChainOrderFilled` and `AcknowledgeOnChainFill` keep
  their shapes (the acknowledge command emits one extra event in its batch).
  `Position` `SCHEMA_VERSION` 3 -> 4; `OnChainTrade` and the
  witness/enrich/acknowledge/mark mechanics are unchanged.
- No SQL migration: the set lives inside the aggregate state; the
  `position_view` projection rebuilds from events and its generated columns
  reference only unchanged JSON paths.
- The set is empty at rest and bounded by the in-flight jobs (a couple at most,
  even across the bot and the `process-tx` CLI). A crash between mark and settle
  leaks one entry, pruned by the next re-delivery's settle (bot) or the next CLI
  re-run's refuse-branch settle; the leak is harmless to correctness (it only
  makes an already-accounted fill un-re-appliable) and self-heals.
- `SettleOnChainFill` propagates infrastructure errors so the apalis retry
  re-drives the settle until the entry self-heals. Under sustained infra failure
  the job dead-letters (operator-visible) with one entry still in the set --
  loud and bounded, consistent with the rest of the pipeline's at-least-once
  contract, never a silent double-count.
