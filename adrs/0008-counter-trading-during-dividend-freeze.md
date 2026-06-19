# ADR 0008: Counter-trading (broker hedging) during a dividend freeze

- **Status:** Proposed
- **Date:** 2026-06-15
- **Linear:** RAI-1055 (parent RAI-1033 — Dividends Issuance + Liquidity Bots
  MVP, milestone M1 — wtStock NAV bump MVP)
- **Related:** RAI-1038 (liquidity rebalance pre-flight / freeze guard),
  RAI-1037 (issuance status endpoint), RAI-586, RAI-1039 (wtStock NAV bump)

## Context

A **dividend freeze** suspends supply-changing operations on a tokenized equity
while a corporate action (dividend, split) bumps the wrapper's NAV, so the
backing cannot be reconciled at a stale ratio. The freeze is the coordination
mechanism that prevents minting/redeeming at the wrong NAV during that window.

The freeze is deliberately **narrow** (st0x.issuance `SPEC.md`, "Freeze
invariant — frozen is not de-listed"):

- It gates **new mints** (issuance rejects with `AssetFrozen`) and **new
  liquidity-initiated rebalancing** (the RAI-1038 guard skips frozen assets at
  the rebalancing trigger).
- It does **not** halt: in-flight mints/redemptions, redemption detection, NAV
  reads, or **on-chain secondary-market trading**.

The question (RAI-1055): the freeze stops _supply_ changes, but the liquidity
bot also **hedges market-making fills** — it places an offsetting Alpaca trade
to neutralise the directional exposure created when a taker fills one of the
standing Raindex orders. That hedging path is independent of rebalancing and
keeps running during a freeze. Should it also stop?

### Architectural facts that constrain the answer

These were established by reading the current code, not assumed:

1. **The bot does not manage the on-chain Raindex market-making orders.** Its
   only orderbook writes are vault deposits/withdrawals for rebalancing
   (`crates/raindex/src/lib.rs` exposes `withdraw`/`submit_deposit`/
   `submit_withdraw`/`confirm_tx` — no `addOrder`/`removeOrder`). The standing
   limit orders are placed and priced **externally**; the bot only _reacts_ to
   fills (`OrderFillMonitor` watches `ClearV3`/`TakeOrderV3`). **The bot cannot
   stop fills**, because it does not control the orders that produce them.

2. **Hedging is reactive and is the only delta-neutraliser.** Pipeline: fill →
   `AccountForDexTrade` → witness/acknowledge to `Position` →
   `check_execution_readiness` → `preflight_counter_trade` → place Alpaca order
   (`src/conductor.rs`, `src/trading/onchain/trade_accountant.rs`,
   `src/onchain/accumulator.rs`).

3. **The `Position` aggregate records every fill unconditionally.** The
   `OnChainOrderFilled` event always mutates `net` (`src/position.rs`),
   _regardless_ of whether a hedge is later placed. So if hedging is suppressed
   while fills continue, `net` accumulates **naked directional exposure** — the
   exact risk the system exists to prevent. Today the only thing that clears a
   suppressed-symbol's exposure is an offsetting taker fill or a manual
   `ManuallyAdjustPosition`.

4. **There is no runtime per-symbol pause.** The per-asset `trading` /
   `rebalancing` `OperationMode`s are static TOML, read at startup only
   (`crates/config/src/loader.rs::is_trading_enabled`); flipping one needs a
   restart. A _dynamic_ per-symbol hedging pause does not exist and would
   require new mutable state + a control surface to set it.

5. **Failure blast radius is global.** A supervised job that exhausts retries
   trips the circuit breaker and halts the **whole conductor** (all symbols, all
   queues) via `on_terminal_failure` → monitor exit (`src/conductor/job.rs`,
   `src/conductor/exit.rs`). There is no per-symbol isolation.

6. **Broker-side unavailability is already handled.** Around ex-dividend Alpaca
   may restrict the underlying; the existing market-hours gate and
   `preflight_counter_trade` skip/defer hedges when the broker can't execute
   (`src/onchain/accumulator.rs`, `crates/execution/.../executor.rs`). No
   freeze-specific handling is needed for that.

## Decision (proposed — pending stakeholder review)

**Do not halt counter-trading/broker hedging during a dividend freeze. Hedging
continues unconditionally; the freeze affects only supply operations (mints and
liquidity-initiated rebalancing, per RAI-1038).**

## Options considered

### Option A — Keep hedging unconditionally (recommended)

The freeze gates supply; hedging is left running.

- **Pro:** Hedging is a _reactive_ neutraliser of fills that already executed
  on-chain. Suppressing it does not stop the fill — it only strips away the
  offset, leaving naked exposure (fact 3). Continuing to hedge keeps the book
  delta-neutral, which is strictly safer.
- **Pro:** Matches the freeze's actual purpose. The freeze prevents minting at a
  stale NAV; a secondary-market fill is an inventory change, not a supply
  change, and never touches the dividend/NAV mechanics.
- **Pro:** Zero new machinery, zero new failure surface (facts 4, 5).
- **Pro:** Even in the stale-price arb scenario (see "Residual risk"), hedging
  _caps_ the loss to the arb spread rather than adding open market risk on top.
- **Con:** Does nothing about the stale-price arb risk on the standing Raindex
  orders during a NAV bump — but that risk is not the hedger's to fix (see
  follow-up).

### Option B — Halt hedging for frozen assets

Add a dynamic per-symbol pause that stops `AccountForDexTrade`/counter-trading
for a frozen symbol.

- **Pro:** Conceptually "stops the bot for the asset" (echoes RAI-586's "if we
  just stop the liquidity bot this would be good enough").
- **Con (decisive):** Because the bot cannot pull the standing Raindex orders
  (fact 1), fills keep happening during the freeze. Suppressing the hedge while
  fills continue **guarantees naked exposure** (fact 3) — it makes risk worse,
  not better.
- **Con:** Requires building the runtime per-symbol pause that doesn't exist
  (fact 4), plus care not to trip the global fail-stop (fact 5).
- **Con:** Still leaves the actual exposure lever (the orders) untouched.

### Option C — Full stop: halt hedging _and_ pull/disable the Raindex orders

Coordinate an external order pull (or reprice) so no fills occur, then it is
safe to also pause hedging.

- **Pro:** The only option that genuinely removes freeze-window exposure: no
  orders → no fills → nothing to hedge.
- **Con:** Order management is **out of this bot's scope** (fact 1) — it needs
  the external market-making system / operator to pull or reprice. It is a
  separate, heavier piece of work, not a liquidity-bot freeze behavior.
- **Con:** Ordering matters: orders must be pulled _before_ hedging is paused,
  or there's an exposure window. This is a cross-system orchestration problem,
  not an M1 hedging-bot change.

## Consequences

- The liquidity bot's freeze behavior (M1) is **rebalancing-only** (RAI-1038).
  Hedging needs no freeze-aware code, so RAI-1055 closes as a decision with no
  liquidity implementation work.
- Delta-neutrality is preserved through a freeze; no naked exposure is
  introduced by the freeze itself.
- The bot keeps placing Alpaca hedges during the freeze; broker-side
  restrictions are absorbed by the existing market-hours/preflight gates.

### Residual risk and follow-up (out of scope for RAI-1055)

During a NAV bump, the _standing Raindex orders_ may quote a stale price and be
picked off by arbitrageurs. That is a **market-making pricing** problem, not a
hedging one, and it lives wherever the orders are managed (external). The
mitigation — pull or reprice the asset's Raindex orders for the freeze window —
is Option C's order-management half. If freeze-window arb is a material concern,
it should be a **separate issue** against the order-management system,
explicitly not the hedging bot. Hedging-continues (Option A) is correct
regardless of how that follow-up is resolved.

## Why an ADR (not just an issue close)

The decision turns on non-obvious architectural facts (the bot doesn't own the
orders; hedging is reactive; suppression creates naked exposure; no runtime
pause; global fail-stop). Recording the reasoning prevents re-litigating
"shouldn't we just stop everything during a freeze?" and clarifies that the real
exposure lever is order management, which sits outside this bot.
