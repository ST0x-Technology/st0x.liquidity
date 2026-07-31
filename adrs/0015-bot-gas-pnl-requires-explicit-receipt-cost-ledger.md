# ADR 0015: Bot gas PnL requires an explicit receipt cost ledger

- **Status:** Proposed
- **Date:** 2026-07-16
- **Linear:** RAI-1406 (parent RAI-1402 -- Extended-hours PnL leaks)

## Context

The `/pnl` endpoint currently reports bot gas as `not_ingested`. RAI-1406 asks
to include bot gas so net PnL is no longer an upper bound.

The existing persisted data is not enough to do that safely:

- `OnChainTradeEvent::Enriched` records `gas_used`, `effective_gas_price`, and a
  Pyth equity reference price for the transaction that produced a Raindex fill.
- A Raindex fill transaction can be submitted by a taker or solver. The existing
  enrichment does not persist the receipt `from` address or a gas-payer
  classification, so it cannot prove the bot paid that gas.
- The enrichment's Pyth price is the traded equity's reference price, not
  ETH/USD. It cannot value native gas in USD.
- Rebalancing, wrapping, vault operations, and CCTP burns/mints are the paths
  where the bot normally pays gas, but their terminal events mostly persist tx
  hashes and economic CCTP/tokenization fees, not normalized gas-cost facts.

Counting existing receipt gas as bot gas would silently overstate costs whenever
the fill transaction was paid by someone else, and valuing gas without a
persisted ETH/USD source would make the PnL report non-reproducible.

## Decision

Do not derive bot gas costs directly from `OnChainTradeEvent::Enriched`.

Bot gas must enter PnL through a dedicated persisted receipt-cost ledger. Each
recorded cost fact must include:

- transaction hash and chain;
- receipt `from` address;
- bot gas-payer classification;
- gas used and effective gas price;
- native token cost in wei;
- ETH/USD price used for valuation, including source and timestamp/block;
- USD cost;
- operation category, such as vault deposit, vault withdraw, wrap, unwrap, CCTP
  burn, CCTP mint, or bot-paid order-management transaction;
- optional symbol when the gas can be directly attributed to an equity.

The `/pnl` report should include only ledger rows classified as bot-paid. Rows
without payer classification or USD valuation remain excluded and should
increment missing-cost diagnostics rather than silently entering PnL.

## Consequences

### Positive

- Prevents solver/taker-paid Raindex fill gas from being charged to the bot.
- Makes PnL reproducible from persisted facts instead of live RPC or live price
  lookups at report time.
- Gives reviewers a clear checklist for the eventual implementation: receipt
  ingestion, payer classification, ETH/USD valuation, and PnL cost inclusion.

### Negative / costs

- RAI-1406 is not completed by simply reading the existing `OnChainTrade`
  enrichment stream.
- A complete implementation needs new persisted cost facts and instrumentation
  around every bot-signed onchain transaction path.
- ETH/USD valuation needs an explicit source decision. The current equity Pyth
  feed configuration is insufficient.

### Neutral

- Existing CCTP `fee_collected` and tokenization fees remain included as they
  are today. Those are economic protocol/provider fees, not native gas costs.
- Existing PnL still reports bot gas as excluded until the ledger is implemented
  and populated.

## Alternatives considered

### Count all enriched Raindex fill gas as bot gas

Rejected. The transaction can be paid by the taker/solver, and the existing
event lacks `from` or any payer classification. This would corrupt PnL by
charging the bot for costs it did not pay.

### Fetch receipts and ETH/USD live inside `/pnl`

Rejected. The endpoint supports `asOfRowid` snapshot semantics for persisted
SQLite events. Live receipt and price lookups would make historical PnL depend
on current RPC availability and mutable external data.

### Use current Pyth equity enrichment as the USD valuation source

Rejected. That price is for the traded equity, not ETH/USD. Gas is paid in the
chain's native token and needs a native-token/USD valuation.

### Attach gas fields ad hoc to each existing aggregate event

Rejected as the primary model. It would duplicate valuation and classification
logic across mint, redemption, USDC rebalancing, wrapper recovery, and future
order-management events. A single receipt-cost ledger keeps PnL ingestion
uniform.

## Follow-ups

- Add a receipt-cost aggregate or append-only ledger table with the fields in
  this ADR.
- Pick and document the ETH/USD valuation source and failure mode.
- Emit receipt-cost facts from every bot-signed transaction path.
- Update `/pnl` to load those facts as `bot_gas` cost entries, mark coverage as
  `included` when rows exist, and keep unclassified/unvalued receipts out of net
  PnL.
