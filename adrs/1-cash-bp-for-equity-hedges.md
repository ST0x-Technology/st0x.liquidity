# ADR 1: Use Alpaca `cash` (not `non_marginable_buying_power`) for equity hedge preflight

## Status

Accepted

## Context

When a Raindex onchain trade leaves a directional exposure, the bot enqueues a
hedge order on Alpaca. Before placing the order, the conductor runs a preflight
check that asks the broker executor: "do we have enough buying power to cover
this buy?"

PR #562 introduced that preflight. It computed available buying power as:

```rust
margin_safe_buying_power_cents =
    min(non_marginable_buying_power, cash)
```

Intent at the time: never let the bot use leverage.
`non_marginable_buying_power` (NM-BP) is Alpaca's "buying power that cannot use
margin" field, and capping it at `cash` was added as belt-and-suspenders against
any account configuration where Alpaca might fluff NM-BP with credit.

### Why this is wrong for our use case

Per Alpaca's documentation (see
<https://alpaca.markets/learn/understanding-unsettled-funds>):

> we cover the float between the time equities are purchased and settled. There
> are no costs associated with this and you don't need to wait for trades to
> settle before using the proceeds to buy more equities.

> unsettled funds cannot be used to purchase cryptocurrency, nor can they be
> withdrawn from an account.

`non_marginable_buying_power` is the **crypto-rules** budget: settled cash only,
because crypto must be fully paid. It's what crypto orders are checked against.

For equity-to-equity trades, Alpaca lets us spend unsettled equity-sale proceeds
immediately with no margin loan; the broker absorbs the T+1 float at no cost.
Those unsettled proceeds are tracked in the `cash` field, not in NM-BP. NM-BP
only picks them up after T+1 settlement.

Concrete failure observed: MSTR hedge skipped with
`insufficient margin-safe buying power: estimated cost 587881 cents exceeds
available 3155 cents`,
while the Alpaca account actually held ~$35,000 in cash from sales executed
minutes earlier. The old preflight refused to spend that money because NM-BP
hadn't picked it up yet.

## Decision

Use Alpaca's `cash` field directly for equity hedge preflight. The invariant "do
not use margin" is enforced by requiring `estimated_cost_cents <= cash`: as long
as the buy cost does not exceed `cash`, Alpaca settles it from cash +
unsettled-float, never extending a loan.

`non_marginable_buying_power` is no longer fetched. If we ever add a crypto
hedge path, that path will introduce its own NM-BP check.

## Consequences

- Hedges fire immediately after equity sales, instead of waiting up to one
  trading day for settlement.
- The 1% slippage buffer (`broker.counter_trade_slippage_bps`) continues to
  cushion against the BP estimate drifting between preflight and order
  placement.
- We remain strictly within Alpaca's documented "no margin loan" boundary for
  equity purchases, so the original PR #562 invariant is preserved.
- The CQRS event `OffchainMarginSafeBuyingPower` is renamed to
  `OffchainCashBuyingPower` and a migration rewrites historical events. The
  historical values were computed with the old formula and are not recalculated
  -- events are immutable facts about what the system observed at the time.

## Out of scope

- A minimum settled-cash floor for fees or transfers. If we later need to
  reserve a buffer of settled cash, that is a separate policy on top of this
  preflight, not a replacement for it.
- Crypto hedging. If introduced, it must use `non_marginable_buying_power` per
  Alpaca's rules.
