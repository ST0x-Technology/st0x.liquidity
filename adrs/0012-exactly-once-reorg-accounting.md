# ADR 0012: Exactly-once reorg accounting across the OnChainTrade/Position boundary

- Status: Proposed
- Date: 2026-06-23

## Context

The reorg-protection work (tracked as RAI-803/804/805) lets the bot reverse a
confirmed fill that a reorg dropped. Detection (`record_reorg` in
`src/onchain/backfill.rs`) drives one reorg through two non-atomic aggregate
writes, exactly mirroring the fill path that
[ADR 0005](0005-exactly-once-fill-accounting.md) already had to fix:

1. `OnChainTradeCommand::RecordReorg` -- keyed by `(tx_hash, log_index)`,
   guarded by `AlreadyReorged` (a trade can be reorged at most once).
2. `PositionCommand::RecordReorg` -- reverses `net` and the relevant
   `accumulated_*`, keyed by the base `Symbol`.

This is not a narrow crash window. It is strictly worse than the pre-ADR-0005
fill path:

- **Permanent under-reversal (critical).** A crash between write 1 and write 2
  leaves the `OnChainTrade` reorged but the `Position` un-reversed. On retry the
  backfill re-detects the removed log and re-runs `record_reorg`, but write 1
  now returns `AlreadyReorged`, which `record_reorg` treats as "fully done" and
  returns `Ok(())` -- the `Position` reversal is skipped **forever**. The
  position's `net`/`accumulated_*` permanently reflect a fill that no longer
  exists. Unlike the pre-ADR-0005 fill bug (recoverable via a later
  re-delivery), the `AlreadyReorged` guard makes this terminal.
- **No defense in depth.** `Position::RecordReorg` unconditionally emits
  `Reorged`; the aggregate accepts a duplicate `trade_id` and would
  double-reverse if the emission order were flipped or the orchestration
  re-drove it.

The dedupe is one-sided (only `OnChainTrade` guards), which is exactly the
pipeline-grained-dedupe failure ADR 0005 diagnosed for fills.

## Decision

Apply the ADR 0005 pattern to reorgs: make the dedupe step-grained, with the
"fully reversed" marker living on the per-trade `OnChainTrade` aggregate.

1. **Reorg-acknowledgement marker on `OnChainTrade`.** Add
   `reorg_acknowledged_at` (set by a new command, emitted only after the
   `Position` reversal succeeds; schema-version bump). `reorged_at` alone means
   "reversal started"; `reorg_acknowledged_at` means "reversal finished on both
   aggregates."
2. **`record_reorg` becomes load-and-resume** on the `OnChainTrade` keyed by
   `(tx_hash, log_index)`:
   - **reorg-acknowledged** -> done, skip;
   - **absent / not reorged** -> `RecordReorg`, then the `Position` reversal,
     then the reorg-ack marker;
   - **reorged but not reorg-acknowledged** -> resume directly at the `Position`
     reversal, then the marker.

   All live cases converge on the pair issued in fixed order: `Position`
   reversal first, and only on success the `OnChainTrade` reorg-ack marker -- so
   the marker is always the immediate follow-up to a successful `Position`
   write, closing the crash window.
3. **Defense in depth: `Position` rejects a `trade_id` it has already reorged.**
   Exactly as
   [ADR 0010](0010-bounded-pending-ack-set-for-cross-process-exactly-once.md)
   documents for fills, a single `last_reorged_trade_id` slot is correct only
   under serialized, in-order processing: it remembers only the last reversed
   trade. An out-of-order crash-resume defeats it -- reorg B is reversed, a
   newer reorg C of the same symbol completes and advances the slot to C, then
   B's crashed job retries; the slot no longer holds B, so the guard misses it
   and B is reversed a second time, corrupting `net`. So `Position` carries a
   bounded `pending_reorged_trade_ids` set (added on the `Reorged` reversal,
   pruned by a `SettleReorg` command once the reorg-ack marker is durable) **in
   addition to** the retained slot, and `RecordReorg` rejects a `trade_id` that
   is a member of the set **or** equals the slot. The slot bridges a reorg
   reversed under pre-set code and left unsettled at the deploy restart (zero
   migration); the set closes the out-of-order window the slot alone cannot.

### Coupled fix: reactor inventory reversal (RAI-804)

The `PositionEvent::Reorged` reactor arm (`src/rebalancing/trigger/mod.rs`) only
nudges an equity check; it leaves the in-memory inventory cache reflecting the
now-reversed fill until the next snapshot poll (~60s), during which an equity
check can fire an incorrect mint/redemption. The fill events update inventory
directly; the reorg reversal must too. This requires `PositionEvent::Reorged` to
carry the original fill's `price_usdc` so the reactor can reverse both the
equity and the USDC legs (the inverse of the `OnChainOrderFilled` reactor arm),
instead of nudging blind.

`price_usdc` is added to the event as `Option<Float>` with `#[serde(default)]`
for replay safety: a `Reorged` event persisted by the basic detect-and-reverse
path (before this change) has no price, so it deserializes to `None`, and the
reactor falls back to nudging rather than reversing the USDC leg with a
fabricated price -- which a non-optional field defaulting to zero would do,
violating financial integrity.

## Alternatives considered

- **Unbounded `Position`-side set of all reorged `TradeId`s.** Correct without
  the serialization argument, but grows for the life of the symbol -- the
  trade-off ADR 0005 rejected for fills. The chosen `pending_reorged_trade_ids`
  is the bounded form: pruned on `SettleReorg`, so it holds only in-flight
  reversals (empty at rest), exactly as ADR 0010's pending set does for fills.
- **Reorder to Position-first without a marker.** Inverts the failure into
  double-reversal (a crash after the `Position` write re-drives it with no
  dedupe). Strictly worse.
- **Leave as-is, document the gap.** Rejected: the gap is permanent financial
  corruption with no recovery, which violates the financial-integrity rule.

## Consequences

- New `OnChainTrade` reorg-ack event + schema-version bump; absence of the
  marker means "reversal not finished", the resume-safe default for old
  in-flight reorgs.
- `Position` gains the bounded `pending_reorged_trade_ids` set (fed by
  `Reorged`, pruned by a new `ReorgSettled` event via a new `SettleReorg`
  command) alongside the retained `last_reorged_trade_id` slot, and
  `PositionEvent::Reorged` / `PositionCommand::RecordReorg` carry `price_usdc`.
  The set is event-sourced and added with `#[serde(default)]`: it rebuilds from
  events on replay, with the retained slot as the zero-migration bridge for
  reorgs left unsettled across the upgrade -- no extra schema bump or SQL
  migration. This layers the reorg-ack marker, the `Position` dual dedupe
  (slot + set), the prune step, and the resume-based `record_reorg` on top of
  the basic detect-and-reverse behavior.
- The reorg dedupe contract becomes "reorg-acknowledged == done", matching the
  fill contract. Reproduction tests: crash-between-aggregates re-drive recovers
  the reversal; duplicate `Position::RecordReorg` rejected; the stale-inventory
  window closes.
