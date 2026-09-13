# ADR 0021: Use unspread reference marks for dashboard valuation

- Status: Proposed
- Date: 2026-09-07

## Context

The dashboard derives equity USD values from executable Raindex quotes. Those
quotes include a spread and can disappear outside trading sessions. Exposure
then displays as unavailable even when the bot holds a net position.

A symmetric spread cancels when the dashboard averages bid and ask. However, the
pricing model can clamp one side to avoid crossing a previous quote. That
midpoint is no longer the broker reference mark.

The pricing service already holds an unspread broker mark. Its existing
WebSocket and snapshot contracts expose trading quotes, not reference marks.
Grafana's pricing board obtains marks through metrics instead.

The current dashboard subscribes to wrapped assets. Position exposure is in
underlying shares. A valuation contract must distinguish those units when the
wrapper conversion ratio is not one.

## Decision

1. Add a dedicated, authenticated reference-mark snapshot to the pricing
   service. Keep executable quote endpoints and their safety checks unchanged.
2. Identify each underlying equity explicitly. Return its unspread USD mark as a
   precision-preserving decimal, its source timestamp, and fetch time. Also
   expose the wrapped-token identity and conversion ratio needed to value
   wrapped holdings, with the ratio's observation time. Do not infer asset
   identity or valuation units from a ticker prefix alone.
3. Serve the last known mark outside quote-serving sessions. Keep its original
   source timestamp. A successful fetch is not evidence of a new market price.
   Missing marks remain explicitly unavailable; never substitute zero.
4. Use the underlying mark for net-share exposure and unwrapped holdings. Value
   wrapped holdings using the corresponding conversion ratio exactly once.
   Missing conversion data must not hide otherwise valid share exposure.
5. Retain validated marks in the dashboard read model across quote expiry and
   transport failures. Recover marks after a liquidity restart from the pricing
   snapshot, including while markets are closed. Never use a trading quote or
   `Position.last_price` as the valuation fallback.
6. Show the source timestamp and distinguish current, last-known, and
   unavailable values. Surface transport failure separately from market age.
   Reject non-positive prices, invalid identities, future timestamps, and
   regressions. An unchanged observation remains valid as last-known data.
7. Keep this data display-only. Do not change hedge thresholds, order pricing,
   rebalancing, historical accounting, or Position events.

## Consequences

### Positive

- Exposure remains visible across market closures and short pricing outages.
- Our spread and quote protection no longer affect dashboard reference values.
- Underlying shares and wrapped holdings use explicit valuation units.

### Negative / costs

- The fix needs coordinated pricing-service and liquidity changes.
- Last-known values are estimates, not executable liquidation values.
- A cold start during a pricing outage remains unavailable unless a validated
  mark was recovered. This proposal does not add a second durable price store.
- Wrapped valuations also depend on a valid conversion observation.

## Alternatives considered

- Cache executable quote midpoints: preserves spread and unit ambiguity.
- Restore the last fill price: can be old and includes execution effects.
- Read Prometheus metrics in the dashboard: makes monitoring output a financial
  API and lacks an explicit asset and unit contract.
- Read Alpaca directly from the dashboard: duplicates pricing-service duties and
  broker access.

## Follow-ups

1. Approve this contract direction before implementation, per repository policy.
2. Update the dashboard pricing section in SPEC.md and create the implementation
   issue and plan. Finalize the wire schema and freshness policy there.
3. Add and test the pricing endpoint, then update liquidity's read model,
   generated DTOs, and dashboard consumers.
4. Cover asymmetric quote protection, non-unit wrapper ratios, market closures,
   reconnects, cold starts, invalid marks, and timestamp regressions.
5. Check exporter compatibility and stage the pricing endpoint before deploying
   its liquidity consumer. Do not weaken quote-serving rules to restore display.
