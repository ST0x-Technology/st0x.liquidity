# ADR 0002: Adopt Axum and lean on Tower for both transport and business logic

- Status: Proposed
- Date: 2026-05-08

## Context

The HTTP surface of `st0x.liquidity` (dashboard backend, internal control
endpoints, health/metrics, future operator APIs) is currently served by Rocket.
Rocket was originally picked for its ergonomics — declarative routing, request
guards, attribute-driven handlers — and it served that purpose. The project has
since stalled: 0.5.1 shipped in May 2024 and there has been no published release
since, maintainership has shifted, and a 0.6 release candidate has been "in
progress" for over a year. Meanwhile our needs have moved in a direction Rocket
does not serve well:

- Structured tracing per request, with span propagation across async work,
  background jobs, and outbound calls.
- Composable cross-cutting policy (timeouts, concurrency limits, rate limits,
  retries, auth, idempotency) applied uniformly to HTTP handlers and worker
  pipelines.
- Tight integration with the rest of our async stack: Tokio, Hyper,
  `tower-http`, `tracing`, `governor`, `apalis` workers, and the CQRS
  command/handler pipeline.
- Strong compile-time guarantees on application state wiring so that missing
  managed resources fail at build time, not as a 500 in production.

We evaluated Axum, Actix-web, and Rocket. Summary of the relevant differences:

| Dimension                    | Rocket                                | Axum                                       | Actix-web                             |
| ---------------------------- | ------------------------------------- | ------------------------------------------ | ------------------------------------- |
| Latest release (May 2026)    | 0.5.1 (May 2024)                      | 0.8.x active                               | 4.13.x active                         |
| Maintenance                  | Stalled                               | Tokio org, monthly cadence                 | Active under @robjtede                |
| Middleware model             | Fairings (isolated)                   | `tower::Layer` (universal)                 | Custom `Transform` (isolated)         |
| Tower / `tower-http` interop | None                                  | Native                                     | None                                  |
| Compile-time state wiring    | Sentinels (runtime fail-fast at boot) | `State<T>` + `FromRef` (true compile-time) | Runtime                               |
| Generic handlers             | No                                    | Yes                                        | Yes                                   |
| Tracing-span hooks           | Weak                                  | Best of the three                          | Comparable to Axum                    |
| Performance vs. Axum         | -20-40% on most workloads             | Baseline                                   | +5-15% on saturated synthetic benches |

Performance and release-cadence figures are informal community-benchmark / repo
observations as of May 2026 (TechEmpower rounds, GitHub release history); they
are not load-bearing for this decision. The discriminators are interop and
modularity, not throughput.

Performance is not the discriminator for this service. Our hot paths are bound
by RPC, broker latency, database, and serde — not framework overhead. The
discriminators are **interop** and **modularity**: the framework should plug
straight into the rest of our async stack (Tokio, Hyper, `tower-http`,
`tracing`, `apalis`) and let cross-cutting policy compose around both HTTP
handlers and our internal command pipelines without re-implementation. Tower
gives us that; Rocket and Actix do not.

## Decision

1. **Replace Rocket with Axum** as the HTTP framework across the workspace. The
   immediate upstack PRs in this stack carry out that migration.

2. **Adopt Tower (`tower::Service` / `tower::Layer`) as the primary composition
   primitive across the codebase, not just at the HTTP edge.** Cross-cutting
   concerns — tracing spans, structured logging, timeouts, concurrency limits,
   rate limits, retries, idempotency keys, authorization, metrics — are written
   once as Tower layers and applied to:

   - Inbound HTTP handlers (via Axum's `Router::layer`).
   - Outbound HTTP clients built on Hyper / `reqwest`.
   - Internal command pipelines and `apalis` workers, by modeling each command
     handler as a `Service<Command>` and stacking the same layer types we use
     for HTTP.

   This means our business-logic boundaries (e.g. CQRS command handlers, broker
   call wrappers, rebalancer steps) are expressed as `Service`s where it pays
   for itself, so a single `TimeoutLayer` / `TraceLayer` /
   `ConcurrencyLimitLayer` implementation is shared across the system rather
   than reimplemented per surface.

3. **Encode application state via Axum's `State<T>` + `FromRef`** so that any
   handler that depends on a resource (DB pool, broker client, CQRS framework,
   rebalancer, conductor handle) is impossible to wire up incorrectly.
   Forgetting to register a resource becomes a compile error, consistent with
   the project's "make invalid states unrepresentable" stance.

4. **Standardize error handling on `IntoResponse` for HTTP and on a single
   `Service` error type per pipeline for non-HTTP surfaces**, so the same
   `TraceLayer::on_failure` and structured-logging conventions apply uniformly.

## Consequences

### Positive

- One mental model and one set of layer implementations across HTTP handlers and
  worker pipelines. Cross-cutting policy lives in one place per concern.
- Compile-time wiring of state eliminates a class of "forgot to register the
  pool" runtime 500s.
- Direct access to the `tower-http` ecosystem (compression, CORS, request IDs,
  trace, timeouts, decompression, sensitive-headers) without bespoke
  re-implementations.
- Testing improves: `Router` is a `Service`, so handler tests become
  `oneshot(Request::builder()...)` calls — no socket, no server, no fixtures
  beyond the state struct.
- Aligns with the rest of our async stack (Tokio, Hyper, `tracing`, `apalis`)
  maintained by overlapping authors, reducing cross-version surprise.

### Negative / costs

- **Pre-1.0.** Axum is on 0.8.x with 0.9 in development. Plan for one moderate
  migration every 12-18 months. Mitigation: keep Axum-specific code thin and
  centralized; treat handlers as `async fn` over our own extractors where
  practical.
- **Error messages around the `Handler` trait can be cryptic** when handler
  signatures are wrong. Mitigation: use `#[axum::debug_handler]` on non-generic
  handlers; prefer a small set of conventional handler shapes.
- **Tower fluency is now a team requirement.** Writing a custom `Layer` /
  `Service` is more involved than writing a Rocket fairing. Mitigation: document
  the layer authoring pattern in `docs/`, prefer `axum::middleware::from_fn` and
  `tower::ServiceBuilder` for the common cases, and reserve hand-written
  `Service` impls for layers whose state or back-pressure semantics genuinely
  require it.
- **Treating non-HTTP code as `Service`s is a discipline, not a free lunch.**
  Over-Service-ifying small synchronous helpers is worse than leaving them as
  functions. We apply the Tower pattern only where a cross-cutting concern is
  actually shared across surfaces, or where back-pressure / load-shedding /
  timeout semantics are part of the contract.
- **Structured-error logging has the same global-hook gap on Axum that it has on
  every Rust web framework today**: `tower_http::trace::TraceLayer::on_failure`
  sees the response, not the original error. Mitigation: wrap a per-codebase
  `ApiError` enum with explicit span attachment at the conversion site.

### Neutral

- Performance is not expected to change meaningfully in either direction for
  this service.
- Existing Rocket-specific request guards translate to Axum extractors
  (`FromRequestParts` / `FromRequest`) one-to-one.

## Alternatives considered

- **Stay on Rocket.** Rejected: stalled release cadence, no Tower interop,
  weaker tracing hooks. The ergonomic features that made Rocket the original
  choice (declarative routing, request guards, attribute-driven handlers) have
  one-to-one analogues in Axum extractors and `axum::routing`, so the ergonomic
  loss is small relative to the modularity and interop wins.
- **Adopt Actix-web.** Rejected: its middleware ecosystem is a parallel universe
  to Tower, which would force us to write or import every cross-cutting policy
  twice (once for HTTP, once for everything else). The thread-per-core runtime
  model also adds friction to sharing async resources (broker clients, DB pools,
  the CQRS framework) across handlers. The performance margin Actix offers is
  irrelevant to a service whose hot path is broker / RPC / DB bound.
- **Adopt a combinator framework (Warp) or an emerging one (Pavex, xitca-web).**
  Rejected for now: insufficient production gravity and ecosystem fit.
  Reconsider if Pavex matures; the Tower-centric design here ports forward
  cleanly.

## Follow-ups

- Migration PRs: `fix/replace-rocket-with-axum` (this stack) and any downstream
  feature branches that touch the HTTP surface.
- Document the Tower layer authoring conventions and the standard layer stack
  (trace, timeout, concurrency limit, request id, sensitive-header redaction) in
  `docs/`.
- Audit existing CQRS command handlers and broker wrappers for places where
  promoting them to `Service` would let us delete bespoke timeout / retry /
  tracing code, and convert opportunistically — not as a flag-day refactor.
