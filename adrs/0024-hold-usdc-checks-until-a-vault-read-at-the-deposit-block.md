# ADR 0024: Hold USDC checks after a vault deposit until a vault read at the deposit block

- Status: Proposed
- Date: 2026-10-02

## Context

The MarketMaking USDC balance has two writers that reach the inventory view at
different times: transfer settlement and onchain fills.

- **Transfer credit.** An AlpacaToBase transfer deposits USDC into the corridor
  chain's vault. The bot confirms the deposit at the chain tip and settles it
  into the view as soon as `DepositConfirmed` reaches the reactor.
- **Fill debit.** A fill that buys equity with vault USDC reaches the view only
  after `OrderFillMonitor` reads logs up to its ingestion cutoff (`safe` or
  `finalized`, behind the tip), the `AccountForDexTrade` job runs, and the
  `Position` aggregate emits `OnChainOrderFilled`. That lag is tens of seconds.

When a fill in the deposit's block (or an earlier one still behind the cutoff)
spends the deposit, the view holds the credit without the debit. Any USDC check
in that gap sees MarketMaking USDC that no longer exists. On 2026-10-01 an
AlpacaToBase deposit in Base block 52027375 was spent by a fill in the same
block; the bot confirmed the deposit while the fill reader stood at block
52027359, queued a 1,971.318665 USDC BaseToAlpaca transfer from an empty vault
one second later, and applied the fill forty seconds after that. The withdrawal
reverted.

ADR 0018 orders fills against pinned snapshots by block. Nothing ordered a
transfer credit against fills: the credit carried no block.

## Decision

**After an AlpacaToBase credit, hold every USDC check until a vault read pinned
at or past the deposit's block reaches the view.**

1. **Capture the credit's block.** The vault deposit is confirmed through
   `confirm_tx_receipt` (both the fresh path and the `DepositInitiated` resume
   arm), and the receipt's block rides on `ConfirmDeposit` and
   `DepositConfirmed` as `vault_deposit_block: Option<u64>`. The block is read
   from the receipt, never stamped at handling time.
2. **Request a pinned cash read with the credit.** In the same inventory write
   that settles the credit, `complete_usdc_rebalance` calls
   `request_onchain_cash_reconcile(chain, Some(vault_deposit_block))`. This is
   the request a fill whose cash leg underflows already makes (ADR 0018), and it
   reuses all of that machinery:
   - While the request is pending, `is_cash_engaged` makes every USDC check skip
     (transfer triggered, fill triggered, snapshot triggered), before sizing and
     again before dispatch.
   - The next inventory poll of that chain sends `ReconcileOnchainUsdc` pinned
     at the tip. The snapshot aggregate always emits it, so the deduplication of
     an unchanged balance cannot swallow it.
   - The reactor accepts the read only at a block at or past the deposit block.
     It replaces the balance from chain truth, advances the watermark so every
     later fill up to that block is absorbed, resolves the request, and enqueues
     a USDC check.
3. **A pending request keeps its block.** When a request is already pending for
   the chain (for example a fill at a later block underflowed first, or a fill
   without a block underflows after the credit), the new request keeps the
   higher of the two minimum blocks, and a request without a block never drops a
   pending one. A read that misses either block cannot resolve both. The claimed
   read is fetched after the newest request, which covers a request without a
   block.
4. **The request is never below the applied watermark.** A fill underflow can
   force a read in while the transfer is still inflight. A read past the deposit
   block already contains the deposit, so the credit counts it twice, and the
   view rejects any later read below that read's block. The credit's request
   therefore asks for the higher of the deposit block and the chain's applied
   USDC watermark, so a read the view would reject cannot resolve it.
5. **The equity twin keeps its block the same way.** A fill underflow and a mint
   or redemption settlement underflow without a block can both request a read of
   one symbol, so `request_onchain_equity_reconcile` merges the pending block
   exactly as the cash request does.
6. **A fill applied while the read is pending raises its floor.** A fill that
   applies its cash leg cleanly at block B is in the view but not in a read
   pinned below B, so `raise_pending_onchain_cash_floor` moves the pending floor
   to B in the same inventory write. The snapshot reactor evaluates acceptance
   under that write lock too, so a read cannot be accepted between the fill and
   its floor.
7. **Dispatch is atomic with the gate.** A USDC dispatch takes a cash admission
   before sizing and holds the gate's dispatch lock from its last check through
   the enqueue. Every engagement and read request takes that lock exclusively
   and bumps the cash epoch after it is published. Lock order is dispatch lock,
   then inventory: the settlement takes the dispatch lock before its inventory
   write, and a dispatch never touches inventory while it holds the lock.

The request is in memory only. After a restart the view is rebuilt from
snapshots, and a terminal event without tracking defers to the next snapshot
instead of crediting, so there is no credit to hold.

## Tradeoffs & Consequences

**Every AlpacaToBase settlement delays USDC rebalancing by up to one inventory
poll (60 s in production).** The read lands at the next poll, whatever the fill
reader's lag. Rebalancing right after a settlement is rarely urgent, and the
alternative is acting on a balance that may be wrong.

**The cash gate is not chain scoped.** A credit on one corridor holds the USDC
checks of every corridor until its read lands. Production runs one corridor.

**The release read is a forced apply.** The read goes through
`apply_reconciled_onchain_usdc_snapshot`, the recovery path a fill underflow
uses. On every AlpacaToBase settlement it logs
`Force-applying snapshot to recover from error, clearing inflight` at warn with
`DeferredSnapshotReconciliation`, and it zeroes the chain's MarketMaking USDC
inflight. No inflight is expected there: the cash gate holds every new USDC
transfer until the read lands.

**The read, not the fills, releases the hold.** The issue proposed releasing on
the last applied fill. A pinned read at or past the deposit block contains every
fill of that block by construction, so it also covers several fills sharing the
deposit's block, which a fill based release cannot tell apart.

**Legacy events request no read.** A `DepositConfirmed` recorded before this
change, or a receipt without a block, behaves as before.

**Event schema change.** `DepositConfirmed` gains an optional field with
`#[serde(default)]`; persisted events without it deserialize to `None`.

## Alternatives considered

**A. Defer the post settlement check to the next snapshot
(`DeferredToSnapshot`).** Rejected: only the transfer triggered check is
deferred, while fill triggered and periodic checks still read the credit; and a
snapshot of an unchanged balance emits nothing.

**B. Hold until the applied fills reach the deposit block, with a self
rescheduling check.** Tried first and rejected in review. Its snapshot release
depended on an ordinary `OnchainUsdc` event, which the aggregate deduplicates: a
poll that read the post deposit balance while the transfer was still inflight
(rejected by the view) became the aggregate's baseline, so no later poll of that
balance reached the view, and a chain with no later fill held its USDC checks
indefinitely. It also needed new view state, a reschedule loop that a terminal
transfer's `cancel_pending` could erase, and about 220 more production lines.

**C. Hold on the fill reader's checkpoint.** Rejected: the checkpoint marks
fetched blocks, not applied fills; a fetched fill can still be queued behind
other jobs.

**D. Gate the credit instead of the check (delay settling until fills catch
up).** Rejected: the inflight bookkeeping and guard release are tied to the
terminal event, and delaying them would block snapshots (`has_inflight`) for the
same window.
