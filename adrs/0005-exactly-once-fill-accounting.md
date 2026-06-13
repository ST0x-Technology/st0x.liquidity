# ADR 0005: Exactly-once fill accounting across the Witness/Acknowledge boundary

## Status

Accepted

## Context

`process_queued_trade` (`src/conductor.rs`) drives one observed onchain fill
through two non-atomic aggregate writes:

1. `execute_witness_trade` -- `OnChainTradeCommand::Witness`, keyed by
   `(tx_hash, log_index)`.
2. `execute_acknowledge_fill` -- `PositionCommand::AcknowledgeOnChainFill`,
   which moves the position and ultimately triggers the hedge.

Duplicate-input protection is pipeline-grained: the function's first step loads
the `OnChainTrade` aggregate and returns `Ok(None)` if it exists. Two
consequences:

- **Under-delivery (real bug).** A crash or error between write 1 and write 2
  leaves the trade witnessed but the position unaware. Any re-delivery of the
  job (now a real path: orphaned accounting jobs are requeued at startup since
  the RAI-423 fix) short-circuits at the load guard and returns `Ok(None)`. The
  fill is permanently unaccounted: `CheckPositions` cannot see a fill the
  `Position` never learned of, and the only remedy is manual repair via the CLI.
  RAI-966 holds the reproducing tests.
- **No defense in depth.** `Position::AcknowledgeOnChainFill` unconditionally
  emits `OnChainOrderFilled` -- the aggregate itself accepts duplicate
  `trade_id`s and would double-count if the upstream guard is ever bypassed.

Compounding the first point, `execute_acknowledge_fill` (and
`execute_enrich_trade`) swallow command errors -- they log and return unit, so
the apalis job is marked `Done` even when the position write failed, burning the
one delivery the queue guarantees.

## Decision

Make the dedupe step-grained instead of pipeline-grained, tracking
acknowledgement on the `OnChainTrade` aggregate (which is already keyed by the
dedupe identity):

1. Add an acknowledgement marker to `OnChainTrade` (new
   `OnChainTradeEvent::Acknowledged` event emitted by a new `Acknowledge`
   command; schema version bump).
2. `process_queued_trade` resumes instead of skipping: when the aggregate exists
   but is not yet acknowledged, re-run `execute_acknowledge_fill` and mark
   acknowledged on success; only a fully-acknowledged trade short-circuits to
   `Ok(None)`.
3. `execute_acknowledge_fill` propagates errors so apalis retries the job
   instead of silently dropping the fill (`execute_enrich_trade` stays
   best-effort: enrichment is observability data, not financial state).
4. As defense in depth -- and as the correctness guard for the resume path's
   crash window between the position write and the marker write -- `Position`
   rejects a `trade_id` it has already applied. State stays bounded: the
   position remembers only the most recently applied `trade_id`, which is
   sufficient because accounting jobs are serialized per symbol
   (`concurrency(1)` worker plus the per-symbol lock) and re-deliveries are
   drained in enqueue order, so the only trade that can ever be re-driven
   against an unmarked acknowledgement is the most recent one. The upstream
   marker blocks all longer-range duplicates before they reach the position.

## Alternatives considered

- **Position-side trade-id set.** Track every seen `TradeId` in `Position`
  state. Simple, but the set grows unboundedly for the life of the symbol, and
  every duplicate still re-runs the witness path first.
- **Reorder the writes (acknowledge before witness).** Inverts the failure into
  over-delivery: a crash between acknowledge and witness re-delivers a job whose
  position update already happened, and the position has no dedupe to stop the
  double-count. Strictly worse for financial integrity.
- **Transactional outbox across both aggregates.** Correct but requires
  cross-aggregate atomic persistence, which the CQRS framework deliberately does
  not provide (one command, one aggregate, one atomic event batch).

## Consequences

- New `OnChainTrade` event type and schema version bump; existing aggregates
  remain valid (absence of the marker means "not acknowledged", which is the
  resume-safe default for old in-flight trades). `Position` also gains a
  `last_acknowledged_trade_id` field, bumping its schema version.
- The dedupe contract of `process_queued_trade` changes from "exists == done" to
  "acknowledged == done". Chaos scenarios for broker-outage retries
  (RAI-427/RAI-428) were written against this contract: a retry during a broker
  outage re-drives the acknowledge step idempotently and then returns `Ok(None)`
  even when hedge placement is still impossible, so a broker blip does not burn
  the retry budget.
- RAI-966's reproduction tests (duplicate `AcknowledgeOnChainFill` rejected;
  witnessed-but-unacknowledged re-delivery recovers the fill) turn green with
  the RAI-967 fix.
