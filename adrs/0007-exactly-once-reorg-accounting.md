# ADR 0007: Exactly-once reorg accounting across the OnChainTrade/Position boundary

## Status

Proposed

## Context

The reorg-protection slices (RAI-803/804/805) let the bot reverse a confirmed
fill that a reorg dropped. Detection (`record_reorg` in
`src/onchain/backfill.rs`, RAI-805) drives one reorg through two non-atomic
aggregate writes, exactly mirroring the fill path that
[ADR 0005](0005-exactly-once-fill-accounting.md) already had to fix:

1. `OnChainTradeCommand::RecordReorg` -- keyed by `(tx_hash, log_index)`,
   guarded by `AlreadyReorged` (a trade can be reorged at most once).
2. `PositionCommand::RecordReorg` -- reverses `net` and the relevant
   `accumulated_*`, keyed by the base `Symbol`.

An adversarial review of the slices found this is **not** the benign "narrow
crash window" the implementation comment claims. It is strictly worse than the
pre-ADR-0005 fill path:

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
   Add a bounded `last_reorged_trade_id` slot (mirroring
   `last_acknowledged_trade_id`). The backfill worker is `concurrency(1)` and
   processes removed trades sequentially, and the reorg-ack marker skips
   fully-done trades on retry, so the only trade that can be re-driven against
   an unmarked reorg-ack is the most recent one -- a single slot suffices.

### Coupled fix: reactor inventory reversal (RAI-804, high)

The review also found that the `PositionEvent::Reorged` reactor arm
(`src/rebalancing/trigger/mod.rs`) only nudges an equity check; it leaves the
in-memory inventory cache reflecting the now-reversed fill until the next
snapshot poll (~60s), during which an equity check can fire an incorrect
mint/redemption. The fill events update inventory directly; the reorg reversal
must too. This requires `PositionEvent::Reorged` to carry the original fill's
`price_usdc` so the reactor can reverse both the equity and the USDC legs (the
inverse of the `OnChainOrderFilled` reactor arm), instead of nudging blind.

## Alternatives considered

- **Position-side reorged-`TradeId` set.** Bounded-correctness without the
  serialization argument, but grows unboundedly for the life of the symbol --
  the same trade-off ADR 0005 rejected for fills.
- **Reorder to Position-first without a marker.** Inverts the failure into
  double-reversal (a crash after the `Position` write re-drives it with no
  dedupe). Strictly worse, as the review confirmed.
- **Leave as-is, document the gap.** Rejected: the gap is permanent financial
  corruption with no recovery, which violates the financial-integrity rule.

## Consequences

- New `OnChainTrade` reorg-ack event + schema-version bump; absence of the
  marker means "reversal not finished", the resume-safe default for old
  in-flight reorgs.
- `Position` gains `last_reorged_trade_id` (schema-version bump) and
  `PositionEvent::Reorged` / `PositionCommand::RecordReorg` gain `price_usdc`.
  The work lands as a dedicated branch stacked on RAI-805 (mirroring how ADR
  0005's fix was a slice of its own), layering the marker, the dedupe, and the
  resume-based `record_reorg` on top of the basic detect-and-reverse slice.
- The reorg dedupe contract becomes "reorg-acknowledged == done", matching the
  fill contract. Reproduction tests: crash-between-aggregates re-drive recovers
  the reversal; duplicate `Position::RecordReorg` rejected; the stale-inventory
  window closes.
