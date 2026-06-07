# ADR 0003: Reconstruct the USDC rebalancing guard from persisted state on startup

## Status

Accepted

## Context

The USDC rebalancing reactor allows exactly one rebalance at a time, gated by an
in-memory `usdc_in_progress: Arc<AtomicBool>` on `RebalancingService`. It is
claimed when a rebalance is dispatched and released on a _clearable-terminal_
event (success, or a failure that reconciles inflight back to source).

The parent change in this PR ("keep guard on post-USDC-burn failure") makes a
post-burn `BridgingFailed` _keep_ the guard set instead of clearing it, so the
next `UsdcRebalancingCheck` cannot dispatch a fresh burn against funds that CCTP
has already burned. This closes the live and timeout-cleanup paths.

But the guard is in-memory only:

- `usdc_in_progress` is `AtomicBool::new(false)` at construction
  (`RebalancingService::new`) and is never rebuilt from persisted events.
- `usdc_tracking` is an empty `HashMap` at construction; there is no
  `rebuild_usdc_tracking_for_recovery` (unlike mint/redemption).
- USDC inflight bookkeeping and `active_usdc_rebalance` are set by the live
  reactor only and are **not** persisted in `InventorySnapshot`.

So after a process restart that lands between a post-burn `BridgingFailed` and
settlement, the guard is `false`, tracking is empty, and the inventory shows no
inflight. The next imbalance check sees the same imbalance (the burned USDC is
gone from source) and dispatches a **new burn** -- exactly the amplification the
PR exists to prevent. A redeploy mid-incident is precisely when this happens.

The same restart hole exists for any _in-flight_ (not-yet-failed) USDC
rebalance, because USDC bridges have no resume path: `recheck_transfer`'s
`TransferKind::UsdcBridge` arm ("recheck is not supported for USDC bridges") and
`UsdcRebalance::Materialized = Nil` mean no poller or reactor catch-up drives an
interrupted bridge forward after restart.

## Decision

On startup, reconstruct **only the guard** from persisted `UsdcRebalance`
aggregate state. Set `usdc_in_progress = true` if any aggregate is in a
non-clearable-terminal state -- i.e. in progress, or a post-burn
`BridgingFailed` (`burn_tx_hash: Some`). Do **not** reconstruct `usdc_tracking`
or inventory inflight.

Mechanically, mirroring mint/redemption recovery:

- `interrupted_usdc_rebalance_ids(pool)` selects aggregates whose latest event
  is a guard-holding candidate.
- The conductor loads each via the existing `Store<UsdcRebalance>` and asks the
  aggregate state itself, via a new `UsdcRebalance::holds_rebalance_guard()`,
  whether the guard should be held (this resolves the two state-dependent cases:
  `ConversionComplete` is terminal for BaseToAlpaca but in-flight for
  AlpacaToBase; `BridgingFailed` holds the guard only when post-burn).
- If any held, `RebalancingService::recover_usdc_guard()` sets the guard and
  logs at error level (one line at boot, listing the stuck aggregate ids).

## Rationale for guard-only

- **The guard is the primary dispatch gate.** `check_and_trigger_usdc` claims it
  before anything else; re-asserting it durably is sufficient to block re-burns.
- **No settlement events arrive post-restart.** With no USDC bridge
  resume/poller and a `Nil` reactor, an interrupted aggregate emits no further
  events, so the inventory-inflight bookkeeping the live terminal paths would
  consume is never needed. Reconstructing it would be dead state.
- **Avoids financial-misaccounting risk.** Re-deriving inventory inflight on
  restart (when available balances are independently re-hydrated from polled
  on-chain/broker readings) risks double-counting -- the exact class of bug the
  mint recovery path guards against with extensive care. Guard-only touches no
  financial values.
- **Faithful to the in-memory invariant.** The guard means "a USDC rebalance is
  unsettled." We re-derive that boolean from the durable event log instead of
  losing it across a restart.

## Consequences

- A stuck or in-flight USDC aggregate at boot holds the guard until manual
  operator recovery, consistent with the PR's stated "minimum safe behavior"
  (stuck post-burn funds already require manual recovery) and with the fact that
  USDC bridges are not auto-resumed.
- All new USDC rebalancing is blocked while one aggregate is unsettled. This is
  correct: the system only runs one USDC rebalance at a time, and blocking is
  the safe failure mode (the incident was double-burning, not under-burning).
- Terminally-failed post-burn aggregates have no automatic guard clear -- the
  same as the live in-memory behavior today; an operator clears it as part of
  recovery. The boot-time error log surfaces it on every restart until resolved.

**Operator recovery path.** There is no dedicated "clear the USDC guard" CLI
command. The guard is derived state (see below), so the only way to clear it is
to drive the stuck aggregate to a terminal state for which
`UsdcRebalance::holds_rebalance_guard` returns `false` (i.e. confirm the
post-burn funds settled, or otherwise reconcile them), then restart -- recovery
re-derives the guard from the event log and leaves it clear. A purpose-built
"CCTP stuck" aggregate state with an explicit operator recovery/resume command
is the tracked follow-up (RAI-715, see Out of scope). Until it exists, recovery
is the manual reconcile-then-restart procedure above.

## Alternatives considered

- **Full tracking + inventory recovery (mirror mint/redemption fully).**
  Rejected: it reconstructs inventory inflight, carrying double-count risk, for
  no benefit absent a resume path. Larger and riskier than the problem warrants.
- **Persist the guard as its own flag/column.** Rejected: the guard is derived
  state; the `UsdcRebalance` event log is the source of truth. A separate flag
  could drift from the aggregate.

## Out of scope

- Resuming/retrying interrupted USDC bridges after restart (no recheck path
  exists; tracked separately).
- An explicit "CCTP stuck" aggregate state with retry/resume (floated in
  RAI-715, left as follow-up).
