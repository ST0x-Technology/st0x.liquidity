# ADR 0015: Reconstruct pre-burn Alpaca-to-Base USDC inventory before resume

- Status: Proposed
- Date: 2026-07-16
- Issue: RAI-1324
- Supersedes: ADR 0003

## Context

ADR 0003 made startup recovery reconstruct only the durable USDC rebalance
guard. It explicitly rejected rebuilding `usdc_tracking`, the active transfer
owner, or inventory inflight because interrupted USDC transfers had no automatic
resume path at the time. Rebuilding financial state provided no benefit and
risked applying an additive transfer operation on top of independently refreshed
venue balances.

That premise no longer holds. Startup recovery now re-arms resumable
Alpaca-to-Base transfers, including `Withdrawing` and `WithdrawalComplete`, and
the transfer job drives the existing aggregate forward. The corresponding
inventory reservation remains reactor-only: `Initiated` moves the Hedging source
amount from available to inflight in memory, but neither that side effect nor
its ownership is durable.

A restart therefore creates two unsafe pre-burn shapes:

- `ConversionComplete`: a fresh broker snapshot already reflects the USD-to-USDC
  conversion, but the resumed withdrawal later emits `Initiated`. Applying the
  normal additive `Start` effect to that refreshed source balance debits the
  transfer twice and can fail with `InsufficientAvailable`.
- `Withdrawing` or `WithdrawalComplete`: `Initiated` was committed before the
  restart, so resume emits no second initiation event. The inflight reservation
  remains zero, and terminal settlement can fail with `InsufficientInflight`.

The issue's original `state == None` framing is not a safe recovery signal. With
the current state machine, `None` means no aggregate event proves that external
work started. A job row with no aggregate state remains a fresh transfer and
must take the normal initialization path.

Blind reconstruction after the CCTP burn is also unsafe. A startup snapshot may
already contain the funds at the destination while the aggregate still predates
the corresponding event. Adding destination available again when the resume
adopts the completed operation would double-count funds. The safe reconstruction
boundary is therefore the pre-burn Alpaca-to-Base window, where the destination
cannot yet contain rebalance proceeds.

## Decision

Startup recovery will continue to latch the durable USDC guard immediately. It
may enqueue the existing pre-burn Alpaca-to-Base transfer job, but the worker
will not enter the transfer manager until a fresh post-startup Hedging inventory
observation has restored its source reservation.

The inventory poller reports every successful reserve-adjusted offchain cash
fetch to the recovery service, including unchanged values that the durable
snapshot aggregate correctly deduplicates. This callback is the freshness
barrier; snapshot deduplication remains unchanged.

Under the existing USDC event-serialization and inventory ownership locks,
recovery will then establish one exact recovery context for the aggregate:

- `None` remains fresh and receives no reconstructed reservation.
- `ConversionComplete` records that the fresh source snapshot already reflects
  conversion. When the resumed withdrawal emits `Initiated`, the reactor sets
  Hedging inflight to the aggregate's exact initiated amount without debiting
  available again.
- `Withdrawing`, `WithdrawalComplete`, and Alpaca-to-Base `BridgingSubmitting`
  restore the active aggregate owner, tracking context, and exact Hedging
  inflight amount before their transfer job resumes. Available remains the
  freshly observed source balance.

Reconstruction is replacement-based, not additive. Repeating startup recovery or
resuming the same aggregate again sets the same owner and inflight amount, so it
cannot reserve twice. A different active aggregate owner is a conflict and keeps
the global guard latched rather than overwriting financial state.

The re-armed transfer continues through the existing manager and reactor paths.
Subsequent bridge progress fills the normal tracking fields, and terminal
settlement releases the reconstructed inflight through the same operation as a
transfer that never restarted.

If the fresh source observation cannot be obtained, the worker delayed-redrives
without entering the transfer manager and the guard remains held. If the
aggregate cannot be loaded, more than one source reservation claims ownership,
or a different active aggregate already owns inventory, recovery fails closed
and surfaces an error without overwriting financial state. No imbalance check
may dispatch while the recovery context is incomplete.

Post-burn aggregates retain ADR 0003's guard-only behavior unless their existing
state-specific recovery path can prove the complete inventory effect. This ADR
does not infer financial state from a post-burn snapshot.

## Alternatives Considered

### Keep guard-only recovery and tolerate terminal underflow

- Pros: Preserves the smallest startup recovery surface and never rewrites
  inventory.
- Cons: Resumed transfers continue with missing ownership and inflight state;
  RAI-1322 can clear the guard after an underflow but leaves a cross-venue stale
  snapshot window.
- Rejected because: It treats the symptom after settlement instead of preserving
  the accounting invariant needed by the now-supported resume path.

### Replay the normal additive `Start` effect during recovery

- Pros: Reuses the live initiation operation without adding recovery-specific
  state.
- Cons: A fresh source snapshot may already reflect the conversion or
  withdrawal, so subtracting the amount again can underflow or double-debit
  available.
- Rejected because: Recovery must reconcile durable intent with observed venue
  state; replaying a non-idempotent transition cannot do that safely.

### Set inflight before obtaining a fresh source snapshot

- Pros: Lets startup re-arm the transfer immediately.
- Cons: Inflight suppresses later snapshot application, potentially preserving a
  default or stale available balance for the entire resumed transfer.
- Rejected because: The resulting ledger can remain wrong even though the
  inflight underflow disappears.

### Reconstruct every guard-holding state, including post-burn states

- Pros: Gives every resumed transfer a uniform in-memory shape.
- Cons: The destination may already contain minted or deposited funds that the
  aggregate has not recorded. Later adoption and settlement could credit those
  funds a second time.
- Rejected because: Aggregate state plus a point-in-time cross-venue snapshot is
  insufficient to reconstruct post-burn settlement without additional on-chain
  proof.

## Consequences

- Pre-burn Alpaca-to-Base resumes regain the same active-owner and inflight
  invariants as uninterrupted transfers.
- Genuine retries and repeated startup recovery are idempotent because recovery
  sets an exact reservation rather than adding another reservation.
- Startup re-arm now depends on a fresh Hedging snapshot. A broker outage delays
  recovery and keeps USDC automation blocked, which is the safe failure mode.
- The recovery context becomes an explicit part of the USDC reactor contract:
  live initiation debits available, while post-snapshot recovery establishes
  inflight without a second debit.
- Post-burn recovery remains deliberately conservative. Extending exact
  inventory reconstruction across the burn boundary requires a separate decision
  backed by transaction and destination-settlement evidence.
