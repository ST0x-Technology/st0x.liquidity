# ADR 0004: Model identifiers by what determines them; derive the string form

- Status: Accepted
- Date: 2026-06-04

## Context

Identifiers we exchange with external systems were frequently represented as
`String`, or as a newtype that merely wraps `String`. The old
`ClientOrderId(String)` in `crates/execution/src/order/mod.rs` was the
motivating example: it is the idempotency key forwarded to Alpaca, and it was
built three ways —

- `from_uuid(Uuid)` — the string is a single `Uuid`.
- `from_prefixed_uuid(prefix: &'static str, Uuid)` — the string is a fixed
  prefix drawn from a closed set (`"preflight-"`, `"cli-"`) concatenated with a
  `Uuid`.
- `new(impl Into<String>)` — an arbitrary, length-validated string.

Every constructor collapses structured inputs into an opaque `String` and
discards the structure. Nothing can recover the prefix or the `Uuid` from a
stored value; the prefix set is enforced only by whichever `&'static str`
literals callers happen to pass; and `new` admits any string. The newtype gives
the appearance of a domain type while remaining stringly-typed — the invariants
that actually determine a valid value (which prefixes exist, that the body is a
`Uuid`) live nowhere in the type.

## Decision

A value whose string form is _determined by other values_ is modeled as those
values, and the string is **derived** from them via a conversion. The string is
never the stored source of truth.

1. **A string built from N inputs is a struct of N typed fields.** The rendered
   string comes from a `Display`/`From` conversion over the struct. The struct —
   not the string — is what we store and pass around.
2. **A value drawn from a fixed, finite set of literals is an enum** (a single
   constant is a unit variant). Static prefixes like `"preflight-"` and `"cli-"`
   become variants of a closed enum, not `&'static str` arguments.
3. **A `String` newtype is acceptable only when the string is genuinely opaque**
   — it arrives fully formed from outside our control (a broker order id we read
   back) and we never construct or destructure it ourselves. Even then it owns a
   parser/validator, not a bare wrapper.

For `ClientOrderId` this is now an enum over its real shapes — a bare `Uuid`, or
the CLI-prefixed `Uuid` variant — with `Display` producing the wire string and
`FromStr`/serde preserving the wire-compatible string form. The broker request
field stays a `String`, but only as the _output_ of converting a well-typed
`ClientOrderId` at the SDK boundary, never as an internal carrier.

## Consequences

### Positive

- Invalid identifiers become unrepresentable: an unknown prefix cannot be
  constructed and the body is always a real `Uuid`.
- The prefix set is closed and exhaustively matchable — adding a kind is a
  compiler-enforced change, not a grep for string literals.
- Components can be recovered where it matters, because they were never
  discarded.
- The `String` lives only at the boundary, matching the standing rule to convert
  domain newtypes to SDK primitives inside the callee.

### Negative / costs

- More types and a little conversion code per identifier.
- A migration cost for existing call sites and for any persisted/serialized form
  (serde must (de)serialize via the rendered string to keep wire compatibility).

### Neutral

- The wire format is unchanged; only the in-memory representation gains
  structure.

## Alternatives considered

- **Keep the `String` newtype, add validation in `new`.** Rejected: validating
  in a constructor does not capture _what determines_ the value, leaves the
  prefix set open, and still discards structure.
- **Separate newtypes per shape with no shared type.** Rejected: a caller
  holding "some client order id" would need an ad-hoc enum anyway; model the sum
  type directly.

## Follow-ups

- Audit other `*(String)` identifier newtypes (broker order ids, correlation
  ids) with the same test: is the string determined by values we already hold?
  If so, model the values.
