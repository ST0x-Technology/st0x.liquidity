# ADR 0011: Decouple fill-ingestion latency from reorg safety via first-class reorg handling

- Status: Proposed
- Date: 2026-06-23

## Context

Fill ingestion has moved through three cutoffs. It first capped the backfill
range at `tip - required_confirmations` -- a confirmation-depth guess, not real
finality. [#873](https://github.com/ST0x-Technology/st0x.liquidity/pull/873)
([RAI-1057](https://linear.app/makeitrain/issue/RAI-1057)) replaced that with
the chain's latest **finalized** block (`BlockNumberOrTag::Finalized` in
`src/conductor/monitor/order_fills.rs`), hardcoding the tag because finalized is
the only block tag that fully eliminates reorg exposure; any other tag reopens
it. Moving the cutoff to `safe`
([RAI-1140](https://linear.app/makeitrain/issue/RAI-1140)) trades finality for a
large latency win. None of these cutoffs had an ADR; this one records the
tradeoff and supplies the safety mechanism that makes a sub-finality cutoff
sound.

On-chain fills are on **Base, an OP-stack L2** (`docs/domain.md`, SPEC.md
"Raindex Event Monitor"). On Base the `finalized` tag tracks **L1
finalization**, so it lags the unsafe head by minutes. The measured lag is **~20
minutes / ~601 blocks**, beyond the ~13 min of pure L1 finality because of
OP-stack batch-posting and derivation delay.

That latency is not free. The bot's purpose is to minimize directional exposure
by hedging each on-chain fill on Alpaca as soon as possible. Pinning ingestion
to `finalized` means every fill sits **unhedged for ~20 minutes** between the
on-chain event and the hedge. On volatile equities that is a large, **certain,
continuous** market-risk cost paid on every fill -- to defend against a **rare**
event (a reorg deep enough to drop an already-ingested fill).

The failure mode `finalized` defends against is real: if the bot ingests a fill,
records the `OnChainTrade`, updates the `Position`, and hedges on Alpaca, and
that fill's block later reorgs away, the bot is left holding a **naked
directional position** backing a trade that never happened. `finalized` makes
that impossible on a single chain, at the cost of the latency above.

The current design therefore **couples two independent concerns into one knob**:
ingestion latency and reorg safety. Choosing the safest tag forces the slowest
latency; there is no way to buy back latency without reintroducing reorg risk.

The block tags available on Base, with their latency and reorg exposure:

| Tag               | Latency on Base        | Reorg exposure                                   |
| ----------------- | ---------------------- | ------------------------------------------------ |
| `latest` (unsafe) | ~seconds (2s blocks)   | sequencer can reorg the unsafe head              |
| `safe`            | ~batch interval (mins) | only if L1 reorgs the batch before finalization  |
| `finalized`       | ~20 min (observed)     | none (barring catastrophic L1 consensus failure) |

First-class reorg handling -- detect a reorged fill and reverse its effect on
both aggregates -- is the proposed safety mechanism this ADR adopts. SPEC.md
still lists it under "Future Consideration: Reorg Handling" and the only merged
protection is the finalized-block gate itself
([#656](https://github.com/ST0x-Technology/st0x.liquidity/pull/656)/[#873](https://github.com/ST0x-Technology/st0x.liquidity/pull/873)).
The design lives in the [RAI-294](https://linear.app/makeitrain/issue/RAI-294)
stack: [#864](https://github.com/ST0x-Technology/st0x.liquidity/pull/864)
(block_hash for fork detection,
[RAI-802](https://linear.app/makeitrain/issue/RAI-802)),
[#867](https://github.com/ST0x-Technology/st0x.liquidity/pull/867)
(`OnChainTradeEvent::Reorged`,
[RAI-803](https://linear.app/makeitrain/issue/RAI-803)),
[#868](https://github.com/ST0x-Technology/st0x.liquidity/pull/868)
(`PositionEvent::Reorged` reverses the fill's position impact,
[RAI-804](https://linear.app/makeitrain/issue/RAI-804)),
[#869](https://github.com/ST0x-Technology/st0x.liquidity/pull/869) (detection: a
removed past-confirmation log emits `RecordReorg` to both aggregates,
[RAI-805](https://linear.app/makeitrain/issue/RAI-805)), and
[#870](https://github.com/ST0x-Technology/st0x.liquidity/pull/870), whose
exactly-once reorg accounting will be documented in ADR 0012 (the reorg-specific
exactly-once accounting, distinct from ADR 0005's fill-level exactly-once).
Reversing a fill requires two aggregate writes (OnChainTrade and Position); a
crash between them leaves the position un-reversed forever -- the
permanent-under-reversal failure that ADR 0012's exactly-once accounting will
harden against. The reorg-detection logic must never ship without the
exactly-once accounting ADR 0012 will document, or the permanent-under-reversal
failure above becomes reachable in production. The end-to-end chaos proof
([RAI-807](https://linear.app/makeitrain/issue/RAI-807)) and the
block-hash-mismatch detection path on backfill/restart
([RAI-806](https://linear.app/makeitrain/issue/RAI-806)) are not yet written.

The `safe` cutoff is the baseline this ADR builds on. The `safe` cutoff delivers
the latency half of the tradeoff -- the unhedged window drops from ~20 minutes
to roughly the batch interval. But `safe` is not finality: an L1 reorg of the
not-yet-finalized batch can still drop a `safe` fill, so the `safe` cutoff
**reintroduces** the reorg exposure that `finalized` had eliminated, with no
mechanism to detect or reverse it. That is the gap this ADR closes -- moving off
`finalized` re-exposes reorg risk, which is only safe to accept if reorgs are
detected and reversed -- so it pays for that safety mechanism rather than leave
the `safe` cutoff (or any lower one) unguarded.

## Decision

Decouple ingestion latency from reorg safety, and adopt first-class reorg
handling as the safety mechanism. Concretely:

1. **Adopt the [RAI-294](https://linear.app/makeitrain/issue/RAI-294)
   reorg-handling design as the safety mechanism** rather than the `finalized`
   tag. Land the reorg-handling stack
   ([#864](https://github.com/ST0x-Technology/st0x.liquidity/pull/864),
   [#867](https://github.com/ST0x-Technology/st0x.liquidity/pull/867),
   [#868](https://github.com/ST0x-Technology/st0x.liquidity/pull/868),
   [#869](https://github.com/ST0x-Technology/st0x.liquidity/pull/869) +
   [#870](https://github.com/ST0x-Technology/st0x.liquidity/pull/870)) together
   with the exactly-once accounting ADR 0012 will document, plus the outstanding
   detection path ([RAI-806](https://linear.app/makeitrain/issue/RAI-806)) and
   the chaos-proxy e2e ([RAI-807](https://linear.app/makeitrain/issue/RAI-807)).

2. **Make the ingestion cutoff a configured block tag today, with a `tip - N`
   confirmation depth as a follow-up.** Latency becomes an operational knob that
   is independent of correctness; the safety mechanism is reorg
   detection-and-reversal, not the choice of cutoff. The configurable block-tag
   half (`safe` | `finalized`, required and startup-validated in
   `crates/config/src/evm.rs`, no silent default) **is in place**; the
   outstanding half is the `tip - N` depth mode listed in Follow-ups.

3. **Treat the `safe` cutoff as an explicitly time-bounded interim, not the end
   state.** The `safe` cutoff takes the latency win immediately, accepting a
   small, bounded, low-probability reorg window (an L1 reorg of an unfinalized
   batch). This ADR commits to closing that window with the reorg handling above
   -- the next step, not an indefinite TODO. Once detection-and-reversal is
   merged and the chaos-proxy e2e passes, the cutoff depth can be tuned freely
   (including lower than `safe`) because correctness no longer depends on it.

The end state is one mechanism: "ingest at a configurable depth, with any
reorged fill detected and reversed." The `safe` cutoff is the latency half
landing first; this ADR commits to the safety half, so the result is a single
coherent design rather than a `safe`-tag workaround left permanently unguarded.

## Consequences

### Positive

- Hedge latency stops being hostage to finality. Once the default is lowered,
  fills hedge in roughly the batch interval (or N-block depth) instead of ~20
  minutes, cutting the certain per-fill unhedged-exposure window by an order of
  magnitude.
- Latency and safety become independently tunable. Operators can trade latency
  against reorg-window size by config, without touching correctness code.
- Reorg risk is accounted for rather than merely avoided: a reorged fill is
  detected and its effect reversed on both the `OnChainTrade` and `Position`
  aggregates, restoring the internal exposure accounting. This is strictly
  stronger than the status quo even at the `finalized` default, because the
  `finalized` guarantee is "barring catastrophic L1 failure," not "never."
  Caveat: this reverses internal CQRS state only -- an already-placed Alpaca
  hedge is **not** unwound by this mechanism, so residual broker-side exposure
  can persist after a reorg until it is reconciled out of band (see Follow-ups).
- The reorg-handling design is hardened against the crash-window failure (to be
  documented in ADR 0012), so the remaining cost is implementing the
  block-hash-mismatch detection path and the chaos-proxy e2e, not greenfield
  design.

### Negative / costs

- Real, lasting complexity. Reorg handling touches two aggregates
  (`OnChainTrade`, `Position`), adds new events/commands (`RecordReorg`,
  `AcknowledgeReorg`, `Reorged`, with `AlreadyReorged` / `DuplicateReorg`
  guards), three SQLite migrations, and exactly-once accounting that ADR 0012
  will show is subtle (the permanent-under-reversal crash window). This is
  permanent surface area to maintain and reason about.
- The reversal reactor must reverse both the equity and USDC legs, so
  `PositionEvent::Reorged` has to carry the original fill's `price_usdc`; replay
  safety forces it to be an optional, defaulted field. More state, more replay
  edge cases.
- A new operational concern: reorg events need alerting and a runbook. The
  reversal path itself must be correct under crash/retry, which is exactly the
  class of bug ADR 0012 will exist to close.
- The chaos-proxy e2e ([RAI-807](https://linear.app/makeitrain/issue/RAI-807))
  is non-trivial test infrastructure that must be built and kept green for the
  guarantee to mean anything.

### Interim state (`safe` cutoff without reorg handling)

Until the [RAI-294](https://linear.app/makeitrain/issue/RAI-294) stack lands,
the `safe` cutoff runs unguarded. These interim gaps are what this ADR's
decision addresses; they are not negligible while the window is open, and all
three exist whenever the `safe` cutoff runs without reorg handling:

- **Naked hedge on reorg.** A `safe` fill is recorded, hedged on Alpaca, then
  dropped by an L1 reorg of the unfinalized batch, leaving a naked directional
  position backing a trade that never happened (the failure `finalized`
  prevented).
- **Missed fill on quiet-skew cutoff regression (under-hedge).** When the `safe`
  cutoff regresses within `SAFE_CUTOFF_QUIET_SKEW`, `poll_once` skips the tick
  (`CutoffWithinQuietSkew`) without re-scanning the regressed range
  (`src/conductor/monitor/order_fills.rs`), so a fill that becomes canonical on
  the new chain within that range is permanently missed.
- **Removed-log dropped without reversal.** `backfill_range` filters out
  `removed: true` logs at/below the cutoff (`src/onchain/backfill.rs`); if a
  hedge was already placed against such a fill there is no reversal path, so a
  reorged-and-replaced log leaves a naked position.

The quiet-skew and removed-log gaps are asymmetric -- they under-hedge -- unlike
the naked-hedge case. Reorg detection-and-reversal (this ADR's decision)
restores the internal aggregate accounting for the **naked-hedge** and
**removed-log** cases: each ingests a fill, hedges it, then loses the block, so
reversing the recorded fill restores the `OnChainTrade` and `Position` exposure
accounting to match the canonical chain. It does **not** unwind the
already-placed Alpaca hedge, so residual broker-side exposure persists until
reconciled out of band (operator/alerting; see Follow-ups). It does **not**
recover the **quiet-skew miss** at all -- `poll_once` skips the regressed range
without re-scanning, so that fill is never ingested and there is nothing to
reverse. Recovering it needs the skipped range re-scanned, which is a separate
follow-up (see Follow-ups), not the reversal path.

### Neutral

- `required_confirmations` keeps its current, narrower meaning
  (transaction-submission paths only); the ingestion depth is a separate knob,
  so the two are not conflated again as they were before
  [#873](https://github.com/ST0x-Technology/st0x.liquidity/pull/873).
- The startup block-tag-support probe is already cutoff-aware: the probe
  (`probe_cutoff_block_support`) applies the `AliasesChainTip` check only for
  `finalized` (skipped for `safe`). The remaining follow-up is extending it when
  a `tip - N` depth mode is added, not making the existing tag probe mode-aware.

## Alternatives considered

- **Stay on `finalized` (keep reorg handling descoped).** Zero reorg risk, zero
  added complexity. Rejected because it pays a certain ~20-minute unhedged
  window on every fill -- a continuous market-risk cost directly opposed to the
  bot's reason for existing -- to avoid a rare tail event. For a hedging system
  that tradeoff is backwards. This remains the fallback if the team judges the
  complexity not worth it.

- **Stop at the `safe` tag with no reorg handling.** Moving the cutoff to `safe`
  for the latency win is the interim this ADR endorses. _Stopping there_ --
  `safe` with no detection or reversal -- is the rejected option: `safe` blocks
  on Base can still be reorged by an L1 reorg of the batch, and with no handling
  that produces a silent, permanent naked position in the tail, the worst kind
  of failure for a money-handling system. `safe` is a fine _default depth_;
  leaving it unguarded indefinitely is not.

- **Lower the depth and only detect-and-halt (no automated reversal).** Detect a
  reorged fill, halt ingestion, and reconcile manually. Cheaper than full
  reversal, but halting a continuously-running hedging bot is itself an outage,
  manual reconciliation is error-prone under real money, and detection alone is
  already most of the work of the reorg-handling design. Rejected in favor of
  automated reversal, which avoids the outage and manual-reconciliation risk.

## Follow-ups

- Land the [RAI-294](https://linear.app/makeitrain/issue/RAI-294) stack as one
  coherent unit:
  [#864](https://github.com/ST0x-Technology/st0x.liquidity/pull/864),
  [#867](https://github.com/ST0x-Technology/st0x.liquidity/pull/867),
  [#868](https://github.com/ST0x-Technology/st0x.liquidity/pull/868), and
  [#869](https://github.com/ST0x-Technology/st0x.liquidity/pull/869) **with**
  [#870](https://github.com/ST0x-Technology/st0x.liquidity/pull/870) (to be
  documented in ADR 0012) -- never
  [#869](https://github.com/ST0x-Technology/st0x.liquidity/pull/869) without
  [#870](https://github.com/ST0x-Technology/st0x.liquidity/pull/870), because
  reorg detection without exactly-once accounting permits the
  permanent-under-reversal failure (a crash between the two aggregate writes
  leaving the position un-reversed).
- Detection must live in the production `backfill_range` path rather than the
  now-test-only `backfill_events` path that predated the finalized-block cutoff.
- Implement the outstanding work:
  [RAI-806](https://linear.app/makeitrain/issue/RAI-806) (block-hash-mismatch
  detection on backfill/restart) and
  [RAI-807](https://linear.app/makeitrain/issue/RAI-807) (chaos-proxy fork
  simulation), and add the north-star reorg e2e (`tests/e2e/reorg.rs`).
- Close the **quiet-skew miss** path separately: when the `safe` cutoff
  regresses within `SAFE_CUTOFF_QUIET_SKEW`, `poll_once` skips the tick
  (`CutoffWithinQuietSkew`) without re-scanning the regressed range, so a fill
  that becomes canonical there is never ingested -- reorg reversal cannot
  recover it (nothing was recorded to reverse). The fix is to re-scan the
  skipped range, not the reversal path; tracked separately from the RAI-294
  reorg work.
- Add the `tip - N` depth mode to the EVM config (the `safe` | `finalized` tag
  field is in place, startup-validated with no silent default), and extend
  `probe_cutoff_block_support`'s aliasing guard to cover it.
- The `safe` cutoff is the current baseline; once reorg handling is merged and
  the e2e passes, tune the cutoff depth freely (it no longer gates correctness).
- Add alerting and a runbook entry for emitted `Reorged` events.
- Unwind or compensate an already-placed Alpaca hedge after a reorg: this ADR's
  reversal restores internal aggregate accounting only, so broker-side hedge
  unwind/compensation is future work, reconciled out of band (operator/alerting)
  until then.
- Update SPEC.md: promote the "Future Consideration: Reorg Handling" section to
  specified behavior once the
  [RAI-294](https://linear.app/makeitrain/issue/RAI-294) reorg stack lands.
- Move ADR 0012 to Accepted as part of merging
  [#870](https://github.com/ST0x-Technology/st0x.liquidity/pull/870); this ADR
  (0011) records the strategic decision that justifies adopting it.
