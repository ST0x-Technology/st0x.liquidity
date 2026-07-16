# ADR 0015: Keep fresh-mark quoting gates out of the liquidity bot

- **Status:** Proposed
- **Date:** 2026-07-16
- **Linear:** RAI-1405 (parent RAI-1402 -- Extended-hours PnL leaks)

## Context

At the 4am ET extended-hours session open, the first onchain quote can reflect
the prior session's mark before a fresh extended-hours mark has arrived.
RAI-1405 asks the system to avoid quoting until a fresh mark is observed.

The current liquidity bot does not own that quoting surface:

- `SPEC.md` says Raindex orders are continuously placed and priced externally.
  This bot monitors fills, records positions, hedges offchain exposure, and
  manages vault balances for rebalancing.
- `docs/domain.md` defines the onchain venue the same way: orders are placed and
  priced externally; this bot does not add, remove, or reprice them.
- The code matches the docs. `crates/raindex` exposes vault deposit and withdraw
  operations used by rebalancing, not order add/remove or price-update
  operations. Fill processing begins after a `ClearV3` or `TakeOrderV3` event
  has already executed.

Because the stale-mark loss happens at the standing order's pricing boundary,
the only direct fixes are order-management behaviors: pull the affected order,
reprice it, delay deploying it, or make the order's expression reject stale
marks. Those controls are outside this bot's current responsibility.

## Decision

Do not implement a fresh-mark quoting gate inside the liquidity bot.

RAI-1405 should be handled by the order-management system or order expression
that owns the Raindex quote lifecycle. The liquidity bot remains responsible for
reacting to fills that already happened: durable accounting, broker hedging, and
inventory rebalancing.

## Consequences

### Positive

- Preserves the current service boundary documented in `SPEC.md` and
  `docs/domain.md`.
- Avoids a false mitigation that would make fills less visible or less hedged
  without actually stopping stale onchain quotes.
- Keeps hedging behavior simple: if a fill executes, the bot still accounts for
  it and attempts the offsetting broker trade.

### Negative / costs

- RAI-1405 is not resolved by a code change in this repository.
- The 4am stale-mark exposure remains until the order-management layer or order
  expression gains a fresh-mark gate.
- This stack needs an external follow-up before the parent RAI-1402 risk is
  fully closed.

### Neutral

- Vault rebalancing still may move inventory to and from Raindex, but it is not
  a quote gate. Removing inventory through rebalancing is slower, indirect, and
  not equivalent to disabling a stale standing order at session open.

## Alternatives considered

### Add a quote gate to fill processing

Rejected. Fill processing only runs after the onchain trade has already
executed. Skipping accounting or hedging would hide or increase exposure; it
would not prevent the stale quote.

### Pause extended-hours broker hedging until a fresh mark

Rejected. Broker hedging is reactive to an executed fill. Pausing it would leave
unhedged directional exposure while the standing order can still be filled.

### Use vault withdrawals as a quote gate

Rejected. Vault operations are inventory management, not order lifecycle
management. They are too indirect for a first-minute session-open pricing gate,
and they cannot express "quote again once the mark is fresh" without building a
new order-management protocol around them.

### Add Raindex order management to this bot

Rejected for this issue. That would expand the bot from reactive hedger and
inventory manager into the owner of the onchain quote lifecycle. If the team
wants that boundary change, it needs a separate accepted ADR and spec update
before implementation.

## Follow-ups

- Move RAI-1405 out of the liquidity-bot implementation stack or replace it with
  an issue against the order-management system.
- Specify the order-side freshness rule: what source proves a fresh mark, what
  staleness threshold applies, and how the order should fail closed until the
  mark is fresh.
- If order management moves into this bot later, update `SPEC.md`,
  `docs/domain.md`, and add an accepted ADR before adding order add/remove or
  reprice code here.
