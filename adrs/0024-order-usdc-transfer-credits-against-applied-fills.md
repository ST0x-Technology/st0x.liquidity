# ADR 0024: Order USDC transfer credits against applied fills

- Status: Accepted
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
transfer credit against fills: the credit carried no block, and the view kept no
record of how far fills had been applied.

## Decision

**Hold every USDC check on a corridor while its latest transfer credit is ahead
of the fills applied on that chain.**

1. **Capture the credit's block.** The vault deposit is confirmed through
   `confirm_tx_receipt` (both the fresh path and the `DepositInitiated` resume
   arm), and the receipt's block rides on `ConfirmDeposit` and
   `DepositConfirmed` as `vault_deposit_block: Option<u64>`. The block is read
   from the receipt, never stamped at handling time.
2. **Record both sides in the view, under the writes they describe.**
   - `onchain_usdc_credit_block[chain]` advances in the same write that settles
     an AlpacaToBase credit into the view. A settlement deferred to the next
     snapshot (no tracking, or an inflight underflow) credits nothing, so it
     records nothing.
   - `onchain_usdc_fill_block[chain]` advances in the same write that handles an
     `OnChainOrderFilled`, whether its cash leg applied, was absorbed by a
     pinned snapshot, or was deferred to a forced reconcile (the cash gate holds
     checks in that case).
3. **The hold.** A corridor is held while
   `credit_block > max(fill_block, onchain_usdc_snapshot_block_watermark)`. The
   snapshot term matters: a snapshot pinned at or past the credit's block
   contains every fill up to that block (ADR 0018), and without it a chain with
   no later fill would never release. The hold is read under the same inventory
   read guard as the imbalance, so a check never sees the credit without its
   block. It applies to every USDC check (transfer triggered, fill triggered,
   snapshot triggered), because all of them are the same `UsdcRebalancingCheck`
   job.
4. **Reschedule, not just skip.** A held check schedules another check 10
   seconds later unless one is already waiting (`Pending` or `Queued`). The fill
   or snapshot that lifts the hold does enqueue a check, but enqueues are best
   effort and a later terminal transfer cancels every pending check; the recheck
   keeps the held imbalance from being dropped until the hold lifts.

The state is in memory only. After a restart the view is rebuilt from snapshots,
and a terminal event without tracking defers to the next snapshot instead of
crediting, so there is no credit to order.

## Tradeoffs & Consequences

**Every AlpacaToBase settlement briefly delays USDC rebalancing on its chain.**
The hold lasts until the fill reader passes the deposit's block or the next
pinned poll lands at or past it. Rebalancing right after a settlement is rarely
urgent, and the alternative is acting on a balance that may be wrong.

**The first fill handled in the credit's block releases the hold.** A block's
fills arrive in one backfill range, enqueued in `(block, log_index)` order, and
the order fill worker runs them with concurrency 1, so the rest of that block's
fills follow right behind the first (a retried fill job can fall behind its
successors). A check that runs between two of them reads the later ones as
unapplied. Holding until the fill reader's checkpoint passes the block and no
fill job at or below it is still queued would close that, at the cost of reading
fill job payloads on every check; the issue defined the release as the last
applied fill, and this ADR keeps that.

**Legacy events set no hold.** A `DepositConfirmed` recorded before this change,
or a receipt without a block, behaves as before.

**Event schema change.** `DepositConfirmed` gains an optional field with
`#[serde(default)]`; persisted events without it deserialize to `None`.

## Alternatives considered

**A. Defer the post settlement check to the next snapshot
(`DeferredToSnapshot`).** Rejected: only the transfer triggered check is
deferred, while fill triggered and periodic checks still read the credit; and a
snapshot of an unchanged balance emits nothing.

**B. Hold on the fill reader's checkpoint.** Rejected: the checkpoint marks
fetched blocks, not applied fills; a fetched fill can still be queued behind
other jobs.

**C. Gate the credit instead of the check (delay settling until fills catch
up).** Rejected: the inflight bookkeeping and guard release are tied to the
terminal event, and delaying them would block snapshots (`has_inflight`) for the
same window.
