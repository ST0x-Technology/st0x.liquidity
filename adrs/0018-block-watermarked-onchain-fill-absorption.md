# ADR 0018: Block-watermarked absorption of onchain fill deltas

- Status: Accepted
- Date: 2026-08-03

## Context

RAI-1500 asked whether the MarketMaking venue has the snapshot-vs-fill
double-count race that ADR 0015 closed for the Hedging venue. The answer is yes,
and the window is wider:

- **Delta writer.** `PositionEvent::OnChainOrderFilled` applies a MarketMaking
  equity delta plus a mirrored USDC delta (`src/rebalancing/trigger/mod.rs`, the
  onchain fill arm) — the structural twin of the offchain fill arm.
- **Snapshot writer.** The inventory poller reads `vaultBalance2` at the latest
  block (`crates/raindex/src/service.rs`, `get_vault_balance`) every ~60s and
  emits `OnchainEquity` / `OnchainUsdc` snapshot events that set the
  MarketMaking balances.
- **The race.** A `vaultBalance2` read includes a `ClearV3`/`TakeOrderV3` fill
  the moment its transaction lands. The fill delta arrives only after
  `OrderFillMonitor` polls `eth_getLogs`, the apalis `AccountForDexTrade` job
  runs, and the `Position` aggregate emits the event — a lag of seconds to
  minutes, versus the broker's ~100ms window offchain. Any inventory poll in
  that lag snapshots the post-fill vault balance; the delta then re-applies the
  same fill on top. Loud variant: `InsufficientAvailable` underflow. Silent
  variant: a wrong balance that drives needless rebalancing until the next poll.
- Mint/redemption/bridge writers are already guarded: they set inflight, and
  `has_inflight()` blocks snapshots for their whole window. Trade-fill deltas
  are the unguarded second writer. `OnchainUsdc` lacks even the per-symbol
  watermark that `OnchainEquity` has.

The comment in `equity_snapshot_would_apply`'s empty `Venue::MarketMaking => {}`
arm currently implies the onchain venue has no second writer. That claim is
wrong and is corrected as part of this change.

One asymmetry against the offchain problem decides the design: **both writers
derive from the same chain**. A vault read is unambiguous _given its block
number_ — a snapshot at block N provably contains every fill at block <= N and
provably excludes every fill at block > N. The offchain fix had no such causal
signal and had to fall back to gating snapshots on order state and host-clock
readings. Here the exact signal exists; today it is simply discarded (nothing
records the read's block, and the fill events do not carry the fill's block).

## Decision

**Guard the delta, not the snapshot.** Skip an `OnChainOrderFilled` delta leg
iff a MarketMaking snapshot already provably contains it:

> equity leg skipped iff
> `fill.block_number <= onchain_snapshot_block_watermark[symbol]` USDC leg
> skipped iff `fill.block_number <= onchain_usdc_snapshot_block_watermark`

Concretely:

1. **Pin and record the read block.** The Raindex service fetches the latest
   block number first and pins every `vaultBalance2` `eth_call` of that poll to
   it, returning the block alongside the balances. Pinning also fixes a latent
   inconsistency: summing vaults across unpinned calls can straddle a block
   boundary. Precedent: ADR 0017 pins Pyth reads the same way.
2. **Carry the block through the snapshot commands.** `OnchainEquity` and
   `OnchainUsdc` commands and events gain `block_number: Option<u64>` (optional
   for legacy-event tolerance). This is the before-read-capture lesson from ADR
   0015 applied onchain: the block is captured with the read, never stamped at
   command-handling time.
3. **Watermarks in the view.** `InventoryView` gains
   `onchain_snapshot_block_watermarks: HashMap<Symbol, u64>` and
   `onchain_usdc_snapshot_block_watermark: Option<u64>`, advanced when the
   respective snapshot applies. In-memory only (the view has no table); a legacy
   event without a block advances nothing.
4. **Thread the fill's block into the event.**
   `PositionCommand::AcknowledgeOnChainFill` and
   `PositionEvent::OnChainOrderFilled` gain `block_number: Option<u64>`, sourced
   from the `eth_getLogs` log that produced the fill (`OnchainTrade` already
   carries it). Optional for legacy tolerance: a fill without a block applies
   unconditionally (current behavior).
5. **The guard.** The onchain fill arm skips a delta leg covered by the
   corresponding watermark, logging at `info!` — an absorbed fill is normal
   operation, not an error. The legs are checked independently: the equity
   watermark is per-symbol, the USDC watermark venue-level.

Snapshots are never blocked on this venue. The `Venue::MarketMaking => {}` arm
in `equity_snapshot_would_apply` stays empty by design — its comment is updated
to point here, and the pinning test
`marketmaking_snapshot_unaffected_by_offchain_order_guards` is retained with its
rationale corrected (snapshots stay unguarded because the _delta_ yields, not
because no second writer exists).

## Why not the offchain design

- **No gate to transplant.** Offchain guard 1 keys on "a hedge order is open" —
  the bot placed the order and knows its lifecycle. Onchain fills are
  taker-initiated: the bot learns about them after the fact, so there is no
  pre-fill state to gate snapshots on.
- **Timestamps are the wrong tool here.** A guard-2-style
  `fetched_at < last_onchain_fill_applied_at` comparison would work (both
  host-clock readings) but is strictly worse than the block comparison: it
  blocks snapshots (starvation risk, the ADR 0015 downside) during a delta lag
  that is minutes rather than ~100ms, and it is approximate where the block
  number is exact. Choosing timestamps when a causal ordering exists would
  repeat alternative D's mistake at a larger scale.
- **Deliberately preserved constraint:** snapshot timestamps remain non-causal
  for fill deltas. The existing tests
  (`fill_delta_before_snapshot_timestamp_is_still_applied`,
  `offchain_fill_delta_before_snapshot_timestamp_updates_equity_and_cash`) pin
  that decision and are untouched — this design orders by block number, never by
  comparing a fill's wall-clock time against a snapshot's.

## Tradeoffs & Consequences

**Accepted downside: legacy events get no protection.** Fills and snapshots
persisted before this change carry no block number, so replays and the first
polls after deploy behave exactly as today. The race stays open until one
post-deploy poll and one post-deploy fill have flowed through; no repair of
history is attempted.

**Freshness is never sacrificed.** Unlike the offchain guards, nothing here
skips a snapshot, so this cannot create an RAI-1502-style staleness problem on
the onchain venue — which is also why RAI-1502's bounded-reconciliation work
deliberately scopes the onchain venue out.

**A skipped delta leg loses its sub-poll freshness contribution.** When the
equity leg is absorbed but the USDC leg is not (or vice versa), each leg is
judged on its own watermark; an absorbed leg's information is already in the
balance by definition, so nothing is lost — the asymmetry is correct, not a gap.

**Event-schema changes.** Steps 2 and 4 add optional fields to persisted events
(`InventorySnapshot` stream and `Position` stream). Both are additive with
`#[serde(default)]`-style tolerance; serde tests assert legacy JSON against
`json!()` literals, and `nix run .#prodVerifyMigrations` gates the deploy as
usual.

**Reorg caveat.** A reorg deeper than the fill's confirmation depth could in
principle reorder a fill relative to a pinned read. Fills are already processed
at the confirmation depth the bot requires everywhere else
(`REQUIRED_CONFIRMATIONS`), and the poller reads at latest; a reorg that
invalidates the comparison would also invalidate the fill event itself, which
existing machinery owns. No additional handling here.

## Alternatives considered

**A. Transplant the offchain guards (gate MarketMaking snapshots).** Rejected:
no open-order signal exists to gate on (see above), and blocking snapshots
during a minutes-long delta lag maximizes the starvation downside ADR 0015
accepted only for a ~2s window.

**B. Host-clock guard 2 only (skip snapshots fetched before the last applied
fill).** Rejected: covers only the reverse ordering, still blocks snapshots, and
is approximate where an exact causal signal exists.

**C. Do nothing; document the overlap as impossible.** Refuted by measurement:
the delta lag is the `OrderFillMonitor` poll interval plus confirmations plus
queue latency, and the 60s inventory poll lands inside it routinely.
