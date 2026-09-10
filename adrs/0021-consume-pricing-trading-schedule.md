# ADR 0021: Consume the pricing trading schedule

- Status: Accepted
- Date: 2026-09-10

## Context

Independent quoting and flattening buffers can drift. Stopping publication also
does not revoke cached quotes or signed orders. The existing liquidity policy
skips ordinary overnight closures.

## Decision

Pricing owns each eligible scope's execution cutoff. Liquidity polls its
authenticated schedule and stores conservative interval boundaries before using
them. Enabled mode flattens net exposure before every actual closure, including
overnight gaps. Observation mode records decisions without changing the existing
hedge policy. Rollout starts in observation mode after a supporting binary
release. Runtime configs retain their existing schema until then; tested
observation-mode fragments live in `docs/trading-schedule/`.

Each asset has an explicit scope and eligibility profile. Broker execution
permission remains independent. Regular sessions retain market orders; extended
sessions retain protected limit orders and the reference chain from ADR 0019.
Missing pricing data never disables ordinary risk-reducing hedging. QSEP, FTF,
and CBRS are absent from the pricing registry and remain isolated in degraded
unpriced scopes, retaining their existing session eligibility. This fallback
does not establish complete schedule coverage.

The poll interval is 5 seconds, request timeout 3 seconds, response freshness 30
seconds, maximum calendar age 7200 seconds, and permitted future evidence skew 2
seconds. Each value is explicit configuration. A 900-second emergency buffer
applies only without a usable schedule and with a trusted broker close.

The earliest accepted cutoff and close survive outages and restarts. Conflicting
revisions cannot postpone them. A fresh non-overlapping interval can replace the
latch only after its predecessor closes and the new interval opens.

Startup rejects a persisted scope whose identity, asset membership, eligibility,
or profile revision no longer matches configuration. This also covers
broker-only fallback windows. Scope changes require an explicit migration of
retained safety state; renaming a scope never clears its deadline automatically.
Rollback-adjusted timers use the same persisted progress as placement and
flattening, while queue identities retain the original boundary timestamp for
idempotency.

Timers wake a one-shot position scan without creating additional periodic scan
chains. Fill ingestion, reconciliation, and cancellation confirmation continue.
Cash, inventory, quantity, and price limits remain unchanged. With schedule
enforcement enabled, Pending-orphan recovery looks up the original client order
ID before session selection or reference pricing and reconciles accepted orders
after closure. An unaccepted orphan without a durable limit price waits for
regular hours. Residual exposure remains visible while that intent is pending.
Observation mode retains the legacy recovery path.

## Consequences

This removes normal runtime dependence on matching buffer values. It does not
guarantee that every hedge fills or grant overnight trading eligibility.
Continuous eligible sessions use their next actual closing boundary.

The connection uses the existing Google service-account identity and pricing
origin. There is no new shared secret. Pricing outages retain earlier deadlines;
they do not independently authorize aggressive liquidation.

The metadata token parser follows Google's
[instance identity token contract](https://docs.cloud.google.com/compute/docs/instances/verifying-instance-identity#token_contents):
three JWT parts, a string audience URI, and an expiration in Unix seconds. Its
payload uses the unpadded base64url encoding defined by
[RFC 7515 section 2](https://www.rfc-editor.org/rfc/rfc7515#section-2). These
locally inspected claims bound cache reuse; pricing verifies the signature.

Recovery follows Alpaca's
[lookup by client order ID contract](https://docs.alpaca.markets/us/reference/getorderbyclientorderidforaccount).
A 404 means currently not found, not proof that an order was never accepted or
that the broker retains all historical IDs forever. A retry keeps the original
client order ID and passes current broker and schedule guards. Recovery requires
explicit session terms: missing `extended_hours`, or an extended order without a
valid limit price, leaves the intent Pending instead of guessing regular hours.
Contract tests use synthetic responses; they are not captured production orders.

Activation requires separate approval, verified scope mapping, and signed-order
expiry evidence. Earlier deadlines cannot revoke signatures already issued.
Rollback must preserve latches and cannot silently restore overnight exposure
handling while claiming all-closure protection.
