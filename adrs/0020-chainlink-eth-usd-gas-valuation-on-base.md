# ADR 0020: Value bot-paid gas via Chainlink ETH/USD on Base

- **Status:** Accepted
- **Date:** 2026-08-20
- **Supersedes:** ADR 0017
- **Linear:** RAI-1625

## Context

ADR 0017 selected Pyth's Base ETH/USD feed for the immutable USD valuation
stored with every bot-paid gas receipt. Pyth is no longer maintained for this
bot, and its pull-based Base feed is stale because nobody pushes updates. The
same obsolete integration also enriches a subset of equity fills with reference
prices that have no production consumer.

Gas costs must remain part of cost-inclusive PnL. Replacing Pyth therefore
requires a live ETH/USD source without weakening the existing reproducibility,
audit, or bounded-retry behavior.

## Decision

1. Use Chainlink's standard ETH/USD proxy on Base through
   `AggregatorV3Interface`. The required plaintext `[bot_gas_valuation]`
   configuration contains the proxy address. The configured production and
   staging address is `0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70`, published in
   [Chainlink's Base feed registry](https://data.chain.link/feeds/base/base/eth-usd).
2. Read `latestRoundData()` and `decimals()` at one pinned Base block. For a
   Base receipt, pin both calls to the receipt block hash with EIP-1898. For an
   Ethereum receipt, pin both calls to the latest Base block number observed by
   the recording job. Persist that Base block number with the valuation.
3. Reject a non-positive answer, a zero update timestamp, an update timestamp
   that cannot be represented as a UTC instant, or a decimals value above the
   supported financial scaling bound. Staleness remains observable rather than
   blocking: persist `updatedAt` as the observation time and warn when it is
   more than 25 minutes older than the receipt. This allows the feed's 20-minute
   heartbeat plus five minutes for publication and receipt-recording latency.
4. Preserve the existing `RecordBotGasReceiptCost` delivery contract. RPC and
   contract-call failures redrive every 30 seconds for a bounded ten-minute
   window; invalid feed data consumes the normal job retry budget and
   dead-letters. Trading and rebalancing never wait on gas-cost recording.
5. Remove per-equity Pyth configuration and runtime reads. Stop issuing new
   `OnChainTradeCommand::Enrich` commands and therefore stop appending new
   `OnChainTradeEvent::Enriched` events. Retain the historical event variant,
   state fields, and dashboard formatting so production event streams continue
   to deserialize, replay, and display without migration or data loss.
6. Remove the Pyth ABI, Nix derivation, Foundry dependency, and module wiring
   once no runtime caller remains.

## Consequences

### Positive

- Bot gas keeps contributing to cost-inclusive PnL using a maintained Base
  ETH/USD feed.
- Receipt valuation remains tied to an auditable Base block and source
  timestamp.
- The bot stops making stale, unused equity-oracle reads and sheds the complete
  Pyth build/runtime dependency.
- Historical enriched trades remain readable without rewriting immutable events.

### Negative / costs

- Chainlink becomes an external dependency for gas valuation. A proxy outage,
  removed historical state, or incompatible RPC response can dead-letter a gas
  cost after the existing bounded retry window.
- Base receipt valuation still requires an archive-capable Base RPC with
  EIP-1898 block-hash `eth_call` support.
- Ethereum receipt valuation remains a Base-observed price at recording time,
  not a price pinned to the Ethereum receipt time.
- The 25-minute stale warning is an operational signal, not a hard safety bound;
  stale values are still recorded for audit completeness.

### Neutral

- The persisted bot-gas event schema, PnL rounding contract, idempotency key,
  and receipt-derived immutable facts do not change.
- Legacy `Enriched` events continue to expose the original `pyth_price` field;
  that name is part of immutable historical data, not an active integration.

## Alternatives considered

### Reuse the liquidity order pricing service

Rejected for this change. It supplies equity prices, not an ETH/USD gas
valuation, and would create a cross-service availability and deployment
dependency for an otherwise self-contained receipt recorder.

### Use DIA

Rejected. DIA covers unrelated onchain order contracts and does not provide the
bot with a clearer block-pinned ETH/USD valuation contract than Chainlink's
standard Base proxy.

### Remove USD gas valuation

Rejected. Native ETH cost alone cannot be combined with realized USD PnL, so gas
would silently disappear from cost-inclusive reporting.

### Use an exchange HTTP API

Rejected. It adds credentials, rate limits, and a non-reproducible offchain
lookup while a maintained onchain Base feed is available.

## Follow-ups

- After staging deployment, verify a newly confirmed bot-paid transaction
  produces one `BotGasReceiptCostEvent::Recorded` fact with the Chainlink source
  and that `/pnl` reports bot-gas coverage as included.
