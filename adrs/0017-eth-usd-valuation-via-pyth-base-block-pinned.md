# ADR 0017: Value bot-paid gas in USD via Pyth ETH/USD on Base, block-pinned

- **Status:** Proposed
- **Date:** 2026-07-23
- **Amends:** ADR 0015
- **Linear:** RAI-1520 (parent RAI-1406 -- Extended-hours PnL leaks)

## Context

ADR 0015 requires every bot-gas receipt-cost fact to carry an ETH/USD valuation
with a source and a timestamp/block, and flags the valuation source as an open
decision. ADR 0016 fixed how the fact is persisted (a CQRS event, read under the
same `asOfRowid` boundary as the rest of `/pnl`) but left the valuation source
and its failure mode unresolved.

Gas for every bot-paid path in scope (vault deposit/withdraw, wrap/unwrap, CCTP
burn/mint, the USDC wallet transfer) is paid in the chain's native token, ETH,
on either Base or Ethereum. The bot already reads Pyth's `getPriceUnsafe` on
Base for equity reference prices (`src/onchain/pyth/mod.rs`), pinned to a block
via a historic `eth_call`, so the same pattern is available for ETH/USD.

ETH/USD is the same economic price regardless of which chain the gas was paid
on, so a single Base-hosted feed can value both Base and Ethereum receipts -- no
separate Ethereum price source is needed.

## Decision

1. **Source: Pyth's `getPriceUnsafe` on Base**, read through the same
   contract-address-parameterized helper the equity enrichment path uses. Two
   new required plaintext config fields carry the contract address and the
   ETH/USD feed id (`[bot_gas_valuation]`); both are public values, not secrets.
   The feed id (`0xff61491a...ace`) is Pyth's `Crypto.ETH/USD` feed, per the
   registry at
   https://www.pyth.network/developers/price-feed-ids#pyth-evm-stable.
2. **Block pinning for reproducibility.** A Base receipt pins the read at the
   receipt's own block HASH (not number, unlike equity enrichment): a reorg
   between the receipt fetch and this call, or across a job retry, can replace
   the block at a given height, so pinning by hash keeps the valuation anchored
   to the exact block that actually contains the transaction. This requires the
   configured `base_rpc_url` to accept EIP-1898's object-form block parameter --
   see "Negative / costs" below. An Ethereum receipt has no equivalent Base
   block, so the read pins at the latest Base block at recording time and
   persists that block number alongside the price -- the valuation is
   reproducible from the persisted block even though it was not observed at the
   exact moment of the Ethereum receipt.
3. **Staleness is recorded, not rejected.** `getPriceUnsafe` never reverts on
   staleness; it returns the feed's last stored price and publish time. The
   worker persists both and logs a warning when the publish time is stale
   relative to the receipt's occurred-at time, but always records the cost fact.
   The persisted timestamp makes staleness auditable after the fact without
   blocking recording.
4. **Failure mode: durable job retries, dead-letter on exhaustion.** Recording
   runs as an apalis job (`RecordBotGasReceiptCost`) enqueued after the
   consumer's existing confirmation step. RPC failures (receipt fetch, block
   fetch, Pyth read) delayed-redrive within a bounded budget (20 attempts at a
   30s delay, ~10 minutes) instead of the apalis job's own ~7s exponential
   backoff budget, so a transient RPC blip does not permanently lose the cost
   fact; the redrive dead-letters loudly once that budget is exhausted. A
   non-positive valuation is retryable through the job's normal exponential
   backoff. A `NonBotPayer` receipt or a conflicting record
   (`BotGasReceiptCostError::ConflictingReceiptCost`) is an invariant violation,
   not a transient condition, but the job framework does not special-case it: it
   is returned as an `Err` like any other failure, so it exhausts the same retry
   budget and reaches the same terminal state. The worker runs as a best-effort
   job (no circuit breaker, no conductor-wide fail-stop): a terminal failure
   dead-letters that one receipt and is logged at `error!`, but never blocks or
   slows trading, matching the "failure in cost recording never blocks trading"
   requirement from the RAI-1520 plan.
5. **Bot-gas enqueue failures are classified and redriven through one shared
   mechanism, not hand-rolled per call site.** Every consumer that enqueues a
   `RecordBotGasReceiptCost` job after its own confirm step (equity mint/
   redemption transfers, the wrapped/unwrapped equity-recovery orphan-deposit
   and orphan-wrap confirmation steps, USDC cross-venue transfers, the startup
   tokenization-resume job) can hit a local `QueuePushError` from that enqueue.
   Post-RAI-1520 review found this classification hand-rolled (and, repeatedly,
   missed or mis-applied) at each call site independently.
   `crate::bot_gas::redrive` centralizes it: each job's own error type
   implements `BotGasFailureClassifier` with an exhaustive `match` (so a new
   error variant must be explicitly classified to compile), and
   `redrive_on_bot_gas_failure` is the one place that turns a classified failure
   into a delayed redrive (log + `push_with_delay`, returning `Ok(())`) instead
   of a terminal job failure. New call sites route through this function rather
   than re-deriving the redrive mechanics. The wrapped/unwrapped equity-recovery
   aggregates' `DispatchToMint`/ `DispatchToRedemption` handoff to a
   mint/redemption resume does NOT route through this mechanism:
   `WrappedEquityRecoveryJob` has no resume arm for the `Detected` state a
   redrive would land back on, so a bot-gas enqueue failure there is folded into
   the aggregate's ordinary `RecoveryFailed` event instead, permanently losing
   that gas fact (see SPEC.md's bot-gas "Known gaps").

## Consequences

### Positive

- Gas valuation is reproducible from persisted facts (price, source, block)
  instead of depending on a live price lookup at report time, matching ADR
  0015's reproducibility requirement.
- One feed values gas on both chains; no per-chain price source or cross-chain
  price reconciliation is needed.
- Reuses the existing Pyth integration pattern instead of introducing a new
  price provider or SDK.
- A failed valuation never strands trading or rebalancing: it only dead-letters
  the one cost fact for operator attention.

### Negative / costs

- An Ethereum receipt's gas cost is valued using a Base-observed price at
  recording time, not at the moment the Ethereum transaction landed -- a small
  timing mismatch versus a hypothetical Ethereum-native ETH/USD feed. Given gas
  costs are cents-to-low-dollars and PnL treats bot gas as a cost line rather
  than a hedge input, this mismatch is not material. This also means the
  recorded valuation for an Ethereum receipt is deliberately non-deterministic
  across separate recording attempts: two attempts to record the same
  Ethereum-chain receipt (e.g. a crash-recovery resume re-enqueueing the same
  tx) will generally pin at different Base blocks and record different prices;
  this is accepted, not a bug, and idempotency for retries is defined over the
  receipt-derived immutable facts rather than the valuation (see SPEC.md).
- A stale Pyth price (e.g. during a quiet period) is recorded as-is; a reader of
  `/pnl` cost data must consult the persisted publish time to judge staleness
  themselves. No automatic staleness cutoff exists.
- `getPriceUnsafe` reverts with `PythErrors.PriceFeedNotFound` for a feed that
  has never been updated on this deployment (it only skips the staleness check,
  not the missing-feed check); that revert surfaces as a retryable
  `PythError`/RPC failure, indistinguishable from a transient RPC error. A
  misconfigured feed id or contract address is therefore not caught by the
  non-positive-price guard described above -- it dead-letters as an opaque RPC
  failure instead of a clearer "no such feed" signal.
- **A Base receipt's block-pinned read assumes the configured `base_rpc_url` is
  an archive node** (or otherwise retains state for the receipt's block).
  `getPriceUnsafe.block(receipt_block_number)` is a historic `eth_call`; once
  the receipt's block falls outside a pruned/full node's retained state window,
  the call fails (typically a "missing trie node"/"header not found"-shaped RPC
  error). This is not a new assumption introduced by this ADR -- the existing
  equity-enrichment path (`src/onchain/pyth/mod.rs`) reads the same way at a
  trade's fill block and depends on the same node capability -- but it is not
  written down anywhere until now. The failure is not silent: it surfaces as
  `EthUsdValuationError::Pyth` -> `RpcError` through the typed
  `RecordBotGasReceiptCostError::Valuation` variant and is logged at `error!`
  when the best-effort job dead-letters (see decision 4). No fallback to a
  different block is introduced for this failure -- doing so would change which
  block the price is pinned to, undermining the reproducibility this ADR is
  built on -- so a pruned node simply means Base receipt cost recording
  dead-letters until either the node is replaced with one that retains the
  needed history or the operator accepts the gap (see SPEC.md's bot-gas "Known
  gaps").
- **A Base receipt's block-pinned read requires the configured `base_rpc_url` to
  accept EIP-1898's object-form block parameter** (`{"blockHash": ...}`), not
  just the legacy tag/number form: the `eth_call` is pinned at
  `receipt_block_hash`, not `receipt_block_number` (see decision above and
  `src/bot_gas/valuation.rs`'s module doc). This is the only call site in the
  codebase that pins by hash. EIP-1898 has been part of the standard Ethereum
  JSON-RPC spec since 2019 and is supported by every mainstream execution
  client; this deployment's Base endpoint is a self-hosted node (not a
  third-party gateway prone to stripping non-standard params), so this is
  accepted as a low-probability risk rather than mitigated with a startup
  capability probe. If it is ever wrong, the symptom is indistinguishable from
  ordinary RPC flakiness: every Base bot-gas cost fact redrives for the full
  bounded window and then dead-letters (`is_transient_rpc_error` classifies the
  resulting `PythError::Rpc` as transient) -- this note is the first thing to
  check if Base bot-gas costs stop recording entirely with no other explanation.

### Neutral

- The Pyth contract address is now parameterized on `extract_pyth_price` rather
  than hardcoded; existing equity-enrichment call sites continue to pass the
  same Base constant, so their behavior is unchanged.
- ADR 0015's required receipt/payer/valuation/category/symbol fields do not
  change.

## Alternatives considered

### Hard-fail past a staleness threshold

Rejected. A hard cutoff can strand cost facts during genuinely quiet market
periods (e.g. an illiquid feed overnight) when the bot still pays gas for
rebalancing. Recording with a warning keeps every receipt's cost captured and
leaves staleness auditable rather than silently dropped.

### A separate Ethereum-native ETH/USD feed

Rejected. ETH/USD is the same economic price on both chains; maintaining two
configured feeds and reconciling them adds configuration surface and a
cross-chain consistency question for no accuracy gain proportional to the cost
being valued (gas, not principal).

### Fetch price from a centralized exchange API instead of Pyth

Rejected. Introduces a new external dependency, new failure modes, and a new
credential/rate-limit surface, when Pyth's on-chain `getPriceUnsafe` is already
integrated, block-pinned, and reproducible from chain state alone.

### Classify `NonBotPayer`/conflict as a distinct non-retryable job outcome

Rejected for now. The job framework (`crate::conductor::job`) has no "skip
retries" signal short of returning `Ok`, which would misrepresent a real
invariant violation as success. Letting these errors exhaust the normal retry
budget and dead-letter is simpler and still surfaces the failure at `error!` for
operator attention; a dedicated fast-path can be added later if repeated useless
retries on a known-permanent conflict become a problem.

## Follow-ups

- Consumer instrumentation and e2e coverage: done (this PR wires up every listed
  production path and adds a mock Pyth contract for the local anvil chain
  covering the full confirm -> record -> `/pnl` path).
- Track ERC20-approval gas (inside deposit/wrap/CCTP library calls) and
  CLI-initiated transactions as an explicitly separate follow-up; both are out
  of scope for this ADR (see the RAI-1520 plan's sign-off section).
