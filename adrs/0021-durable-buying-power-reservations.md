# ADR 0021: Persist buying-power reservations through live hedge orders

- Status: Accepted
- Date: 2026-09-15

## Context

ADR 0001 requires equity hedge buys to use Alpaca `cash` rather than
`non_marginable_buying_power`, because cash includes unsettled equity-sale
proceeds that Alpaca permits the account to reuse without a margin loan.

Buy preflight currently either approves the complete hedge or skips it. Making
that check return the largest cash-funded partial quantity exposes a reservation
gap: production `CheckPositions` preflights symbols independently, and Alpaca
cash does not describe which part the bot already committed to an accepted but
unfilled buy. Multiple partial hedges can therefore size themselves against the
same cash snapshot. The existing `CounterTradeBatchBudget` does not protect
production because it and its caller are test-only.

The final extended-hours price check has a second gap. It treats any
`CounterTradePreflight::Allowed` as a boolean and discards the reservation. A
partial allowance at the final limit price would therefore still submit the
job's larger scan-time quantity.

## Decision

Every fresh buy placement performs its definitive preflight while holding the
global `counter_trade_submission_lock`. The preflight subtracts buying power
already reserved by live bot buy orders from the current Alpaca cash snapshot.
The reservation total is passed explicitly through the `Executor` and
`OrderPlacer` preflight contracts; the execution crate does not read application
projections.

The accepted reservation cost is a non-negative domain value recorded with the
durable `OffchainOrder` placement intent before the broker call. It remains part
of `Pending`, `Submitted`, `PartiallyFilled`, and `Cancelling` state. Successful
terminal orders do not contribute to the live-reservation sum.

A position's failed-order idempotency anchor is reconciled with Alpaca by client
order ID before another placement is sized. If Alpaca has the anchored order,
the bot recovers that order with its original quantity and reservation rather
than sending a newly sized request under the same key. If Alpaca has no such
order, the position releases the exact stale anchor with a guarded aggregate
command and performs a fresh partial preflight. Broker lookup failures defer the
retry: they never prove that an anchor is absent.

Anchor reconciliation is independent of fresh hedge readiness. The periodic
position sweep enqueues recovery-only work for every preserved Alpaca anchor,
including disabled symbols and positions that have since moved below threshold.
When the broker order exists, a guarded recovery claim bypasses the fresh-order
threshold so the already-real side effect is polled and accounted using its
original direction, quantity, and reservation. When it does not exist, the
recovery-only job releases the anchor and leaves any fresh hedge decision to the
normal readiness path.

Periodic and inline discovery share a live-job guard per symbol, so a slow or
rate-limited broker lookup cannot accumulate parallel recovery chains. A
terminal job does not latch the guard: if the exact anchor remains, a later scan
may retry it. If a crash occurred before the durable offchain placement intent
was written, no broker call could have happened; startup and periodic recovery
therefore release that missing local order instead of preserving an impossible
broker anchor.

Legacy events and projections default the reservation to absent. A legacy live
Alpaca buy makes the account budget unknown and blocks unrelated fresh buys
until it resolves; the buy job defers successfully so this expected rollout
state cannot exhaust the shared worker's retry budget. Silently treating an
unknown commitment as zero would permit overspending during rollout. A legacy
failed anchor is first reconciled with Alpaca: an existing order is recovered
from the broker's accepted terms, while an absent order permits guarded release
and fresh sizing.

Fresh regular-hours hedge jobs now preflight at placement time. Fresh
extended-hours jobs continue to preflight against their exact submitted limit
price, but return and apply the allowed reservation rather than reducing the
result to a boolean. Idempotent recovery of an existing pending order does not
preflight again because the durable order already owns its reservation and the
broker may already own the request under its client order ID.

Partial quantity sizing searches integer broker quantity units and accepts only
a candidate whose conservatively rounded, slippage-buffered cost is within the
remaining cash. Fractional candidates below Alpaca's $1 reference notional are
deferred. Any unhedged remainder stays in the position and is eligible for the
normal periodic retry when it still meets the execution threshold.

Quantity precision is a shared preflight-and-placement contract. Regular-hours
orders allow nine decimal places only for fractionable assets. Extended-hours
orders allow them only when the asset is fractionable and Alpaca reports
fractional extended-hours support; otherwise both stages use whole shares.

## Consequences

- Concurrent bot hedge attempts cannot reuse cash committed to another live buy,
  including across process restarts.
- ADR 0001 remains intact: unsettled equity-sale proceeds remain usable, while
  durable reservations are subtracted as a separate bot-owned constraint.
- A partially filled order conservatively retains its complete reservation until
  terminal reconciliation. This can delay another buy but cannot overspend cash.
- All fresh broker placements continue to serialize on the existing global lock.
  The added projection read and preflight increase time under that lock.
- Scan-time preflight remains useful as an early filter, but only placement-time
  preflight is authoritative.
- A stale failed anchor cannot reserve cash forever merely because a broker
  submission once failed locally; absence is established by Alpaca before the
  aggregate releases it.

## Alternatives considered

- **Executor-only partial sizing.** Rejected because independently queued jobs
  can size against the same cash, and the extended-hours job discards a reduced
  final reservation.
- **Use `non_marginable_buying_power`.** Rejected because it revokes ADR 0001's
  explicit ability to hedge with unsettled equity-sale proceeds.
- **Keep reservations only in memory.** Rejected because a restart would erase
  commitments while broker orders remain live.
- **Fetch and value every Alpaca open order.** Rejected for this issue because
  the invariant concerns bot hedge attempts, whose exact conservative cost is
  already known at preflight. Persisting that fact avoids another external API
  dependency and valuation policy.
- **Blindly retain every failed anchor.** Rejected because a local failure
  before broker acceptance would leak buying power indefinitely. Targeted
  client-order lookup resolves the ambiguous submission without valuing
  unrelated orders.
