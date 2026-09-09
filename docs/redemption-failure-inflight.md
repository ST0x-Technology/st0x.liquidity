# Redemption failure and inflight inventory

The runtime and startup contracts differ deliberately (SPEC.md, Failure Handling
and operator reconciliation):

- A live `DetectionFailed` or `RedemptionRejected` releases runtime inflight so
  other rebalancing can proceed. It never credits the stranded shares to either
  venue's available balance. The failed aggregate retains quantity and history.
- Startup seeds unreconciled stranded redemptions into inflight. Operator
  reconciliation prevents that aggregate from being seeded on the next restart;
  it does not change the running process's balances.

Do not depend on the pending-provider poll having observed a request before its
terminal failure. A quick rejection can occur entirely between polls; relying on
disappearance from a previous poll leaves inflight stuck in that case. Terminal
handling drops ownership before capturing its clearing timestamp, then clears
inflight and records snapshot suppression using that same timestamp. Suppression
also protects against a stale pending snapshot entering inventory error
recovery, which can reset the ordinary rebalancing watermark. The recovery path
holds the suppression lock through reset and application, using the same
suppression-to-inventory lock order as terminal clearing.

Recovery restores a released transfer's inflight without debiting available
again. A positive startup-seeded inflight total already includes the failed
transfer; preserve the total so completing one redemption subtracts only its
quantity and leaves other stranded redemptions intact. A positive total smaller
than the recovered transfer is an accounting error. Reject recovery while
another mint or redemption owns the symbol, including a job that has claimed its
guard before its first event. Retain the generation guard through recovery
dispatch. Failed recovery dispatch restores the prior inflight state, including
any startup-seeded exposure.

Loading a failed aggregate does not prove its terminal reactor has finished: the
event store commits before dispatching inline reactors. Before rebuilding
tracking, provider-completion recovery sends
`SynchronizeProviderCompletionRecovery` through the same aggregate store. This
command emits no event. The store's per-aggregate command lock makes it wait for
the preceding command and its reactors. It accepts only a failed redemption
whose tokens were sent, rejecting an already completed or reconciled transfer
before recovery changes inventory.

The rebuild then takes the redemption event mutex to serialize with timeout
cleanup. Release that mutex before sending the recovery command: holding it
across a store send would deadlock with the inline reactor that needs it.
Timeout cleanup and failed-dispatch rollback clear the redemption's chain slot;
they must preserve inflight held on other chains.
